// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

extent_server::extent_server() {
    pthread_mutex_init(&kvs_map_m_, 0);
}

int extent_server::put(extent_protocol::extentid_t id, std::string buf, off_t off, int &)
{
    if (buf.size() >= MAX_FILE_SIZE) {
        return extent_protocol::FBIG;
    }

    ScopedLock m(&kvs_map_m_);
    printf("put %016llx %ld\n", id, off);

    std::map<extent_protocol::extentid_t, fileinfo>::iterator it = kvs_map_.find(id);
    if (it == kvs_map_.end()) {
        fileinfo info;
        info.buf.append(buf);
        info.st.atime = time(NULL);
        info.st.ctime = time(NULL);
        info.st.mtime = time(NULL);
        info.st.size = buf.size();
        kvs_map_[id] = info;
    } else {
        fileinfo &info = it->second;
        if (off == -1)  {
            info.buf.append(buf);
        } else if (off == -2) {
            info.buf = buf;
        } else {
            off_t len = info.buf.size();
            off = max(min(off,len), 0);
            info.buf.replace(off, buf.size(), buf);
        }
        info.st.mtime = time(NULL);
        if (!(id & 0x80000000)) {
            info.st.ctime = time(NULL);
        }
        info.st.size = info.buf.size();
    }

  return extent_protocol::OK;
}

int extent_server::append(extent_protocol::extentid_t id, std::string buf, int &)
{
    if (buf.size() >= MAX_FILE_SIZE) {
        return extent_protocol::FBIG;
    }

    ScopedLock m(&kvs_map_m_);
    printf("put %016llx\n", id);

    std::map<extent_protocol::extentid_t, fileinfo>::iterator it = kvs_map_.find(id);
    if (it == kvs_map_.end()) {
        return extent_protocol::NOENT;
    } else {
        fileinfo &info = it->second;
        info.buf.append(buf);
        info.st.mtime = time(NULL);
        info.st.size = info.buf.size();
    }

  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, off_t off, std::string &buf)
{
    ScopedLock m(&kvs_map_m_);
    printf("get %016llx\n", id);
    std::map<extent_protocol::extentid_t, fileinfo>::iterator it = kvs_map_.find(id);
    if (it == kvs_map_.end()) {
        return extent_protocol::NOENT;
    }

    fileinfo &info = it->second;
    info.st.atime = time(NULL);
    buf = info.buf;
  return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  // You replace this with a real implementation. We send a phony response
  // for now because it's difficult to get FUSE to do anything (including
  // unmount) if getattr fails.
    ScopedLock m(&kvs_map_m_);
    std::map<extent_protocol::extentid_t, fileinfo>::iterator it = kvs_map_.find(id);
    if (it == kvs_map_.end()) {
        printf("no attr key for %016llx\n", id);
        return extent_protocol::NOENT;
    }

    fileinfo &info = it->second;

  a.size = info.st.size;
  a.atime = info.st.atime;
  a.mtime = info.st.mtime;
  a.ctime = info.st.ctime;
  return extent_protocol::OK;
}

int extent_server::setfilesize(extent_protocol::extentid_t id, unsigned long size, int&)
{
    ScopedLock m(&kvs_map_m_);
    printf("setfilesize %016llx %lu\n", id, size);
    std::map<extent_protocol::extentid_t, fileinfo>::iterator it = kvs_map_.find(id);
    if (it == kvs_map_.end()) {
        return extent_protocol::NOENT;
    }

    fileinfo &info = it->second;
  info.st.size = size;
  info.st.mtime = time(NULL);
  if (size < info.buf.size()) {
      info.buf.erase(size);
  }
  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t parent, std::string name, int &)
{
    ScopedLock m(&kvs_map_m_);
    printf("remove %016llx %s\n", parent, name.c_str());

    std::map<extent_protocol::extentid_t, fileinfo>::iterator it1 = kvs_map_.find(parent);
    if (it1 == kvs_map_.end()) {
        return extent_protocol::NOENT;
    }

    fileinfo& info = it1->second;
    std::string &buf = info.buf;
    int i = sizeof(extent_protocol::extentid_t);
    int start = i;
    const char *raw = buf.data();
    while (i < buf.size()) {
        if (raw[i] == '\n') {
            if (i - start == name.size()
                && strncmp(name.data(), raw + start, name.size()) == 0) {
                printf("found %s, remove it\n", name.c_str());

                extent_protocol::extentid_t eid;
                memcpy(&eid, raw + start - sizeof(extent_protocol::extentid_t), sizeof(extent_protocol::extentid_t));

                //ino + name + '\n'
                buf.erase(start - sizeof(extent_protocol::extentid_t), i - start + sizeof(extent_protocol::extentid_t) + 1);

                info.st.mtime = time(NULL);
                info.st.ctime = time(NULL);
                info.st.size = buf.size();

                std::map<extent_protocol::extentid_t, fileinfo>::iterator it = kvs_map_.find(eid);
                if (it == kvs_map_.end()) {
                    printf("no file in map, it a bug!!!\n");
                } else {
                    kvs_map_.erase(it);
                }
                return extent_protocol::OK;
            }
            i += sizeof(extent_protocol::extentid_t) + 1;
            start = i;
        } else {
            i++;
        }
    }
  return extent_protocol::NOENT;
}
