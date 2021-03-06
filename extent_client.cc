// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

// The calls assume that the caller holds a lock on the extent

extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
	make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, off_t off, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  ret = cl->call(extent_protocol::get, eid, off, buf);
  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;
  ret = cl->call(extent_protocol::getattr, eid, attr);
  return ret;
}

extent_protocol::status
extent_client::setfilesize(extent_protocol::extentid_t eid,
    unsigned long size)
{
  extent_protocol::status ret = extent_protocol::OK;
  int r;
  ret = cl->call(extent_protocol::setfilesize, eid, size, r);
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf, off_t off)
{
  extent_protocol::status ret = extent_protocol::OK;
  int r;
  ret = cl->call(extent_protocol::put, eid, buf, off, r);
  return ret;
}

extent_protocol::status
extent_client::remove( extent_protocol::extentid_t parent, std::string name)
{
  extent_protocol::status ret = extent_protocol::OK;
  int r;
  ret = cl->call(extent_protocol::remove, parent, name, r);
  return ret;
}


