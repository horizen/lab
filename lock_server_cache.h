#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>
#include <vector>
#include <map>
#include <set>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_client.h"

class lock_server_cache {
 private:
    struct lockinfo {
        unsigned long req_id;
        lock_protocol::lockid_t lid;
    };

    std::map<lock_protocol::lockid_t, unsigned long> lock_map_;
    std::map<int, lock_client*> rpcc_map_;
    std::vector<lockinfo> revoker_queue_;
    std::vector<lock_protocol::lockid_t> retryer_queue_;
    std::map<lock_protocol::lockid_t, std::set<int> > retryer_map_;
    pthread_mutex_t lock_map_m_;
    pthread_rwlock_t rpcc_map_rwl_;
    pthread_mutex_t revoker_queue_m_;
    pthread_mutex_t retryer_queue_m_;
    pthread_mutex_t retryer_map_m_;
    pthread_cond_t  revoker_queue_cond_;
    pthread_cond_t  retryer_queue_cond_;


 public:
  lock_server_cache();
  virtual ~lock_server_cache();
  lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status subscribe(int clt, std::string, int &);
  lock_protocol::status acquire(int clt, int seq_id, lock_protocol::lockid_t lid, int &);
  lock_protocol::status release(int clt, int seq_id, lock_protocol::lockid_t lid, int &);
    void revoker();
    void retryer();
};

#endif
