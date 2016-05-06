// the caching lock server implementation

#include "lock_server_cache.h"
#include "jsl_log.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

static void *
revokethread(void *x)
{
  lock_server_cache *sc = (lock_server_cache *) x;
  sc->revoker();
  return 0;
}

static void *
retrythread(void *x)
{
  lock_server_cache *sc = (lock_server_cache *) x;
  sc->retryer();
  return 0;
}

lock_server_cache::lock_server_cache()
{
  pthread_t th;
  int r = pthread_create(&th, NULL, &revokethread, (void *) this);
  assert (r == 0);
  r = pthread_create(&th, NULL, &retrythread, (void *) this);
  assert (r == 0);
  assert(pthread_mutex_init(&lock_map_m_, 0) == 0);
  assert(pthread_rwlock_init(&rpcc_map_rwl_, 0) == 0);
  assert(pthread_mutex_init(&revoker_queue_m_, 0) == 0);
  assert(pthread_mutex_init(&retryer_queue_m_, 0) == 0);
  assert(pthread_mutex_init(&retryer_map_m_, 0) == 0);
  assert(pthread_cond_init(&revoker_queue_cond_, 0) == 0);
  assert(pthread_cond_init(&retryer_queue_cond_, 0) == 0);
}

lock_server_cache::~lock_server_cache()
{
    std::map<int, lock_client*>::iterator it;
    for (it = rpcc_map_.begin(); it != rpcc_map_.end(); it++) {
        delete it->second;
    }
    rpcc_map_.clear();
  assert(pthread_mutex_destroy(&lock_map_m_) == 0);
  assert(pthread_rwlock_destroy(&rpcc_map_rwl_) == 0);
  assert(pthread_mutex_destroy(&revoker_queue_m_) == 0);
  assert(pthread_mutex_destroy(&retryer_queue_m_) == 0);
  assert(pthread_mutex_destroy(&retryer_map_m_) == 0);
  assert(pthread_cond_destroy(&revoker_queue_cond_) == 0);
  assert(pthread_cond_destroy(&retryer_queue_cond_) == 0);
}

lock_protocol::status
lock_server_cache::stat(int clt, lock_protocol::lockid_t lid, int &)
{
    return lock_protocol::OK;
}

lock_protocol::status
lock_server_cache::subscribe(int clt, std::string addr, int &)
{
    pthread_rwlock_wrlock(&rpcc_map_rwl_);
    rpcc_map_.insert(std::make_pair(clt, new lock_client(addr)));
    pthread_rwlock_unlock(&rpcc_map_rwl_);
    return lock_protocol::OK;
}

lock_protocol::status
lock_server_cache::acquire(int clt, int seq_id, lock_protocol::lockid_t lid, int &)
{
    jsl_log(JSL_DBG_3, "[server] received acquire request clt=%d seq_id=%d lid=%lld\n", clt,seq_id,lid);
    unsigned long req_id = clt;
    req_id = (req_id << 32) | seq_id;

    pthread_mutex_lock(&lock_map_m_);
    std::map<lock_protocol::lockid_t, unsigned long>::iterator it = lock_map_.find(lid);
      if (it == lock_map_.end()) {
          lock_map_.insert(std::make_pair(lid, req_id));
          pthread_mutex_unlock(&lock_map_m_);
          return lock_protocol::OK;
      } else {
          req_id = it->second;
          pthread_mutex_unlock(&lock_map_m_);

          {
            ScopedLock rwl(&retryer_map_m_);
            std::map<lock_protocol::lockid_t, std::set<int> >::iterator it1 = retryer_map_.find(lid);
            if (it1 == retryer_map_.end()) {
                retryer_map_.insert(std::make_pair(lid, std::set<int>()));
            } else {
                it1->second.insert(clt);
            }
          }

          lockinfo info = {req_id,lid};
          ScopedLock rwl(&revoker_queue_m_);
          revoker_queue_.push_back(info);
          pthread_cond_signal(&revoker_queue_cond_);

          return lock_protocol::RETRY;
      }
}

lock_protocol::status
lock_server_cache::release(int clt, int seq_id, lock_protocol::lockid_t lid, int &)
{
    jsl_log(JSL_DBG_3, "[server] received release request clt=%d seq_id=%d lid=%lld\n", clt,seq_id,lid);

    unsigned long req_id = clt;
    req_id = (req_id << 32) | seq_id;

    pthread_mutex_lock(&lock_map_m_);
    std::map<lock_protocol::lockid_t, unsigned long>::iterator it = lock_map_.find(lid);
      if (it != lock_map_.end()) {
          req_id = it->second;
          int lock_clt = req_id >> 32;
          int lock_seq_id = req_id;
          if (lock_clt == clt && seq_id == lock_seq_id) {
              lock_map_.erase(it);
              pthread_mutex_unlock(&lock_map_m_);

              ScopedLock ql(&retryer_queue_m_);
              retryer_queue_.push_back(lid);
              pthread_cond_signal(&retryer_queue_cond_);

              return lock_protocol::OK;
          } else {
              jsl_log(JSL_DBG_2, "[server] the lock %lld hold by [%d:%d], but your is [%d:%d], no permit release it\n", lid, lock_clt, lock_seq_id, clt, seq_id);
          }
      } else {
          jsl_log(JSL_DBG_2, "[server] %d not hold the lock %lld\n", clt, lid);
      }

      pthread_mutex_unlock(&lock_map_m_);
    return lock_protocol::OK;
}

void
lock_server_cache::revoker()
{
    while(true) {
        pthread_mutex_lock(&revoker_queue_m_);
        while(revoker_queue_.empty()) {
            pthread_cond_wait(&revoker_queue_cond_, &revoker_queue_m_);
        }
        std::vector<lockinfo> revoker_queue_cp(revoker_queue_);
        revoker_queue_.clear();
        pthread_mutex_unlock(&revoker_queue_m_);

        for (int i = 0; i < revoker_queue_cp.size(); i++) {
            lockinfo &info = revoker_queue_cp[i];
            unsigned long req_id = info.req_id;
            int clt = req_id >> 32;
            int seq_id  = req_id;

            pthread_rwlock_rdlock(&rpcc_map_rwl_);
            std::map<int, lock_client*>::iterator it1 = rpcc_map_.find(clt);
            lock_client *cl = NULL;
            if (it1 != rpcc_map_.end()) {
                cl = it1->second;
            } else {
                jsl_log(JSL_DBG_2, "[server] no rpcc client instance of clt %d\n", clt);
            }
            pthread_rwlock_unlock(&rpcc_map_rwl_);

            if (cl != NULL) {
                /* revoke 失败
                 * 1. 未来无其它客户端请求锁，那么会导致当前请求一直阻塞
                 *  解决方法：一旦客户端所有锁请求都满足后，那么自动释放锁
                 * 2. 未来有其它客户端请求锁，会再次触发revoke，正常
                 * 综上可以不用考虑revoke失败的情况
                 */
                jsl_log(JSL_DBG_3, "[server] send revoker request seq_id=%d lid=%lld\n",seq_id, info.lid);
                cl->revoke(seq_id, info.lid);
            }
        }
    }
  // This method should be a continuous loop, that sends revoke
  // messages to lock holders whenever another client wants the
  // same lock

}


void
lock_server_cache::retryer()
{
    while(true) {
        pthread_mutex_lock(&retryer_queue_m_);
        while(retryer_queue_.empty()) {
            pthread_cond_wait(&retryer_queue_cond_, &retryer_queue_m_);
        }
        std::vector<lock_protocol::lockid_t> retryer_queue_cp(retryer_queue_);
        retryer_queue_.clear();
        pthread_mutex_unlock(&retryer_queue_m_);

        for (int i = 0; i < retryer_queue_cp.size(); i++) {
            lock_protocol::lockid_t lid = retryer_queue_cp[i];

next:
            pthread_mutex_lock(&retryer_map_m_);
            std::map<lock_protocol::lockid_t, std::set<int> >::iterator it = retryer_map_.find(lid);
            if (it != retryer_map_.end() && !it->second.empty()) {
                std::set<int>::iterator it2 = it->second.begin();
                int clt = *it2;
                it->second.erase(it2);
                pthread_mutex_unlock(&retryer_map_m_);

                pthread_rwlock_rdlock(&rpcc_map_rwl_);
                std::map<int, lock_client*>::iterator it1 = rpcc_map_.find(clt);
                lock_client *cl = NULL;
                if (it1 != rpcc_map_.end()) {
                    cl = it1->second;
                } else {
                    jsl_log(JSL_DBG_2, "[server] no rpcc client instance of clt %d\n", clt);
                }
                pthread_rwlock_unlock(&rpcc_map_rwl_);

                if (cl != NULL) {
                    /*
                     * retry失败
                     * 1. 未来无客户端发起新的获取该锁的请求，那么所有已经发起该锁请求的,但是返回RETRY的客户端都会阻塞
                     *   解决方法：客户端用pthread_cond_timedwait代替pthread_cond_wait
                     * 2. 有新客户端发起请求那么会重新触发整个流程，无影响
                     * 综上 不用考虑retry失败的情况
                     */
                    jsl_log(JSL_DBG_3, "[server] send retry request lid=%lld\n", lid);
                    int ret = cl->retry(lid);
                    if (ret != lock_protocol::OK) {
                        jsl_log(JSL_DBG_2, "[server] send retry request failure, try next lid=%lld\n", lid);
                        goto next;
                    }
                }
            } else {
                jsl_log(JSL_DBG_3, "[server] no pending acquire request of lock %lld\n", lid);
                if (it != retryer_map_.end()) {
                    retryer_map_.erase(it);
                }
                pthread_mutex_unlock(&retryer_map_m_);
            }
        }
    }
  // This method should be a continuous loop, waiting for locks
  // to be released and then sending retry messages to those who
  // are waiting for it.
}
