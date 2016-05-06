// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <string>
#include <map>
#include "extent_protocol.h"

class extent_server {
private:
    struct fileinfo {
        std::string buf;
        extent_protocol::attr st;
    };

    std::map<extent_protocol::extentid_t, fileinfo> kvs_map_;
    pthread_mutex_t kvs_map_m_;

 public:
   const static int MAX_FILE_SIZE = 1024 * 1024;
  extent_server();
  virtual ~extent_server() {pthread_mutex_destroy(&kvs_map_m_);}

  int put(extent_protocol::extentid_t id, std::string, off_t, int &);
  int append(extent_protocol::extentid_t id, std::string, int &);
  int get(extent_protocol::extentid_t id, off_t, std::string &t);
  int getattr(extent_protocol::extentid_t id, extent_protocol::attr &);
  int setfilesize(extent_protocol::extentid_t id, unsigned long size, int &);
  int remove(extent_protocol::extentid_t parent, std::string name, int &);
};

#endif 







