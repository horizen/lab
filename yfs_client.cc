// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include "lock_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);
  lc = new lock_client_cache(lock_dst);
}

void yfs_client::lock(lock_protocol::lockid_t id)
{
    lc->acquire(id);
}

void yfs_client::unlock(lock_protocol::lockid_t id)
{
    lc->release(id);
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
  std::istringstream ist(n);
  unsigned long long finum;
  ist >> finum;
  return finum;
}

std::string
yfs_client::filename(inum inum)
{
  std::ostringstream ost;
  ost << inum;
  return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
  if(inum & 0x80000000)
    return true;
  return false;
}

bool
yfs_client::isdir(inum inum)
{
  return ! isfile(inum);
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
  int r = OK;


  printf("getfile %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }

  fin.atime = a.atime;
  fin.mtime = a.mtime;
  fin.ctime = a.ctime;
  fin.size = a.size;
  printf("getfile %016llx -> sz %llu\n", inum, fin.size);

 release:

  return r;
}

int
yfs_client::setfilesize(inum inum, unsigned long size)
{
  int r = OK;


  printf("setfilesize %u\n", size);
  if (ec->setfilesize(inum, size) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }

release:
  return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
  int r = OK;


  printf("getdir %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  din.atime = a.atime;
  din.mtime = a.mtime;
  din.ctime = a.ctime;

 release:
  return r;
}


int
yfs_client::get(inum inum, std::string &buf)
{
  int r = OK;

  printf("get %016llx\n", inum);
  r = ec->get(inum, 0, buf);
  return r;
}

int
yfs_client::overwrite(inum inum, const std::string &buf)
{
    int r = OK;

    printf("overwrite %016llx\n", inum);
    return ec->put(inum, buf, -2);
}

int
yfs_client::put(inum inum, const std::string &buf, off_t off)
{
  int r = OK;

  printf("put %016llx\n", inum);
  if (off < 0) { off = 0; };
  return ec->put(inum, buf, off);
}

int
yfs_client::append(inum inum, const std::string &buf)
{
  int r = OK;

  printf("append %016llx\n", inum);
  return ec->put(inum, buf, -1);
}

int
yfs_client::remove(inum parent, const std::string &name)
{
  int r = OK;

  printf("remove %016llx %s\n", parent, name.c_str());
  return ec->remove(parent, name);
}
