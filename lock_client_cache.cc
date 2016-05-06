// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include "jsl_log.h"
#include <sstream>
#include <iostream>
#include <stdio.h>


static void *
releasethread(void *x)
{
  lock_client_cache *cc = (lock_client_cache *) x;
  cc->releaser();
  return 0;
}

int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10));
  last_seq_id = 1;
  const char *hname;
  // assert(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);
  /* register RPC handlers with rlsrpc */
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry);
  subscribe(id);

  pthread_mutex_init(&lock_map_m_, 0);
  pthread_mutex_init(&release_queue_m_, 0);
  pthread_cond_init(&release_queue_cond_, 0);

  pthread_t th;
  int r = pthread_create(&th, NULL, &releasethread, (void *) this);
  assert (r == 0);
}

lock_protocol::status
lock_client_cache::subscribe(std::string addr)
{
    int r;
    int ret = cl->call(lock_protocol::subscribe, cl->id(), addr, r);
    (void)r;
    assert (ret == lock_protocol::OK);
    return ret;
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
    lockinfo *li = NULL;
    int r;
    pthread_mutex_lock(&lock_map_m_);
    std::map<lock_protocol::lockid_t, lockinfo *>::iterator it = lock_map_.find(lid);
    if (it == lock_map_.end()) {
        li = new lockinfo();
        li->waiting = 0;
        li->seq_id = 0;
        li->tid = pthread_self();
        li->retry_received = false;
        li->revoke_received = false;
        pthread_cond_init(&li->wait_cond, 0);
        pthread_cond_init(&li->retry_cond, 0);
        lock_map_.insert(std::make_pair(lid, li));
        pthread_mutex_unlock(&lock_map_m_);

        //没有的话发起rpc调用请求锁
        while (true) {
            int seq_id = last_seq_id++;
            jsl_log(JSL_DBG_3, "[client] send acquire request clt=%d seq_id=%d lid=%lld\n", cl->id(), seq_id, lid);
            int ret = cl->call(lock_protocol::acquire, cl->id(), seq_id, lid, r);
            if (ret == lock_protocol::OK) {
                jsl_log(JSL_DBG_3, "[client] acquire request done clt=%d seq_id=%d lid=%lld\n", cl->id(), seq_id, lid);
                li->seq_id = seq_id;
                return ret;
            }

            assert(ret == lock_protocol::RETRY);

            struct timespec ts;
            //one minute
            ts.tv_sec = time(NULL) + 10;
            ts.tv_nsec = 0;

            int rc = 0;
            pthread_mutex_lock(&lock_map_m_);
            while (!li->retry_received && rc == 0) {
                jsl_log(JSL_DBG_3, "[client] timedwait next acquire request clt=%d seq_id=%d lid=%lld\n", cl->id(), seq_id, lid);
                rc = pthread_cond_timedwait(&li->retry_cond, &lock_map_m_, &ts);
            }
            li->retry_received = false;
            pthread_mutex_unlock(&lock_map_m_);
        }
    } else {
        li = it->second;
    }

    if (li->tid == 0) {
        li->tid = pthread_self();
        li->revoke_received = false;
    } else {
        li->waiting++;
        jsl_log(JSL_DBG_3, "[client] wait acquire request clt=%d seq_id=%d lid=%lld\n", cl->id(), lid);
        pthread_cond_wait(&li->wait_cond, &lock_map_m_);
        li->tid = pthread_self();
        li->waiting--;
        li->revoke_received = false;
    }
    pthread_mutex_unlock(&lock_map_m_);

    return r;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
    jsl_log(JSL_DBG_3, "[client] %d try to release %lld\n", cl->id(), lid);
    pthread_mutex_lock(&lock_map_m_);
    std::map<lock_protocol::lockid_t, lockinfo *>::iterator it = lock_map_.find(lid);
    if (it != lock_map_.end()) {
        lockinfo *li = it->second;

        if (li->waiting == 0 && li->revoke_received) {
            li->revoke_received = false;
            jsl_log(JSL_DBG_3, "[client] lock lt=%d seq_id=%d lid=%lld is freed, releasing\n", cl->id(), li->seq_id, lid);
            lock_map_.erase(it);
            pthread_mutex_unlock(&lock_map_m_);

            ScopedLock ql(&release_queue_m_);
            release_entry re;
            re.lid = lid;
            re.seq_id = li->seq_id;
            re.li = li;
            release_queue_.push_back(re);
            pthread_cond_signal(&release_queue_cond_);
        } else {
            jsl_log(JSL_DBG_3, "[client] cl=%d wake up wait thread for lock lid=%lld\n", cl->id(), lid);
            li->tid = 0;
            pthread_cond_signal(&li->wait_cond);
            pthread_mutex_unlock(&lock_map_m_);
        }
    } else {
        pthread_mutex_unlock(&lock_map_m_);
        jsl_log(JSL_DBG_2, "[client] %d not hold the lock %lld, no permit release\n", cl->id(), lid);
    }

    return lock_protocol::OK;
}

lock_protocol::status
lock_client_cache::revoke(int clt, int seq_id, lock_protocol::lockid_t lid, int &)
{
    jsl_log(JSL_DBG_3, "[client] receive revoke request clt=%d seq_id=%d lid=%lld\n", cl->id(), seq_id, lid);
    pthread_mutex_lock(&lock_map_m_);
    std::map<lock_protocol::lockid_t, lockinfo *>::iterator it = lock_map_.find(lid);
    if (it != lock_map_.end()) {
        lockinfo *li = it->second;
        if (seq_id != li->seq_id) {
            jsl_log(JSL_DBG_2, "[client] seq_id=%d lid=%lld already released\n", seq_id, lid);
            pthread_mutex_unlock(&lock_map_m_);
            return lock_protocol::OK;
        }

        //没有正在等待的线程以及没有正在使用锁的线程
        if (li->waiting == 0 && li->tid == 0) {
            lock_map_.erase(it);
            pthread_mutex_unlock(&lock_map_m_);

            ScopedLock ql(&release_queue_m_);
            release_entry re;
            re.lid = lid;
            re.seq_id = seq_id;
            re.li = li;
            release_queue_.push_back(re);
            pthread_cond_signal(&release_queue_cond_);
        } else {
            li->revoke_received = true;
            pthread_mutex_unlock(&lock_map_m_);
            jsl_log(JSL_DBG_3, "[client] clt=%d seq_id=%d lid=%lld lock is using, please wait\n", cl->id(), seq_id, lid);
        }
    } else {
        pthread_mutex_unlock(&lock_map_m_);
        jsl_log(JSL_DBG_2, "[client] %d not hold the lock %lld, no permit revoke\n", cl->id(), lid);
    }
    return lock_protocol::OK;
}

lock_protocol::status
lock_client_cache::retry(int clt, lock_protocol::lockid_t lid, int &)
{
    jsl_log(JSL_DBG_3, "[client] receive retry request clt=%d lid=%lld\n", cl->id(), lid);
    ScopedLock ql(&lock_map_m_);
    std::map<lock_protocol::lockid_t, lockinfo *>::iterator it = lock_map_.find(lid);
    if (it != lock_map_.end()) {
        lockinfo *li = it->second;
        li->retry_received = true;
        //如果由于网络原因retry没收到会导致acquire请求一直阻塞，但是咱们现在用的是pthread_cond_timedwait所以这个问题不影响
        pthread_cond_signal(&li->retry_cond);
    } else {
        jsl_log(JSL_DBG_2, "[client] %d no pending lock request of lock %lld\n", cl->id(), lid);
    }
    return lock_protocol::OK;
}

void
lock_client_cache::releaser()
{
    while (true) {
        pthread_mutex_lock(&release_queue_m_);
        while (release_queue_.empty()) {
            pthread_cond_wait(&release_queue_cond_, &release_queue_m_);
        }

        std::vector<release_entry> release_queue_cp(release_queue_);
        release_queue_.clear();
        pthread_mutex_unlock(&release_queue_m_);

        jsl_log(JSL_DBG_3, "[client] clt=%d prepare send %d release requests\n", release_queue_cp.size());

        for (int i = 0; i < release_queue_cp.size(); i++) {
            release_entry &re = release_queue_cp[i];
            int r;
            jsl_log(JSL_DBG_3, "[client] send release request clt=%d seq_id=%d lid=%lld\n", cl->id(), re.seq_id, re.lid);
            int ret = cl->call(lock_protocol::release, cl->id(), re.seq_id, re.lid, r);
            jsl_log(JSL_DBG_3, "[client] send release request done clt=%d seq_id=%d lid=%lld\n", cl->id(), re.seq_id, re.lid);
            if (ret == lock_protocol::OK) {
                pthread_cond_destroy(&re.li->retry_cond);
                pthread_cond_destroy(&re.li->wait_cond);
                delete re.li;
            } else {
                jsl_log(JSL_DBG_2, "[client] clt=%d release lock %lld failure\n", cl->id(), re.lid);
            }
        }
    }
  // This method should be a continuous loop, waiting to be notified of
  // freed locks that have been revoked by the server, so that it can
  // send a release RPC.


}

