// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
    assert(pthread_mutex_init(&lock_map_m_, 0) == 0);
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  ScopedLock rwl(&lock_map_m_);
  lock_info* info = lock_map_[lid];
  if (info == NULL) {
      info = new(lock_info);
      assert(info != NULL);
      pthread_cond_t *cond = (pthread_cond_t*)malloc(sizeof(pthread_cond_t));
      assert(cond != NULL);
      assert(pthread_cond_init(cond, 0) == 0);
      info->clt_ = clt;
      info->cond_ = cond;
      info->waiting = 0;
      lock_map_[lid] = info;
  } else {
      ++info->waiting;
      pthread_cond_wait(info->cond_, &lock_map_m_);
      --info->waiting;
      info->clt_ = clt;
  }

  r = ++nacquire;
  return ret;
}


lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;

    ScopedLock rwl(&lock_map_m_);
    lock_info* info = lock_map_[lid];
    if (info == NULL || info->clt_ != clt) {
        r = nacquire;
        return ret;
    }

    if (info->waiting == 0) {
        pthread_cond_destroy(info->cond_);
        free(info->cond_);
        delete info;
        lock_map_.erase(lid);
    } else {
        assert(pthread_cond_signal(info->cond_) == 0);
    }

    r = --nacquire;
    return ret;
}
