// this is the lock server
// the lock client has a similar interface

#ifndef lock_server_h
#define lock_server_h

#include <string>
#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"

class lock_server {

 private:
  struct lock_info {
    int clt_;
    int waiting;
    pthread_cond_t* cond_;
  };
  std::map<lock_protocol::lockid_t, lock_info *> lock_map_;
  pthread_mutex_t lock_map_m_;

 protected:
  int nacquire;


 public:
  lock_server();
  ~lock_server() {pthread_mutex_destroy(&lock_map_m_);}
  lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &);
};

#endif 







