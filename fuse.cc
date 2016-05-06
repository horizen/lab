/*
 * receive request from fuse and call methods of yfs_client
 *
 * started life as low-level example in the fuse distribution.  we
 * have to use low-level interface in order to get i-numbers.  the
 * high-level interface only gives us complete paths.
 */

#include <fuse/fuse_lowlevel.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <errno.h>
#include <sys/stat.h>
#include <arpa/inet.h>
#include "yfs_client.h"


static yfs_client::status fuseserver_lookup_helper(fuse_ino_t parent, const char *name, fuse_ino_t &id);

int myid;
yfs_client *yfs;

int id() { 
  return myid;
}

yfs_client::status
getattr(yfs_client::inum inum, struct stat &st)
{
  yfs_client::status ret;

  bzero(&st, sizeof(st));

  st.st_ino = inum;
  printf("getattr %016llx %d\n", inum, yfs->isfile(inum));
  if(yfs->isfile(inum)){
     yfs_client::fileinfo info;
     ret = yfs->getfile(inum, info);
     if(ret != yfs_client::OK)
       return ret;
     st.st_mode = S_IFREG | 0666;
     st.st_nlink = 1;
     st.st_atime = info.atime;
     st.st_mtime = info.mtime;
     st.st_ctime = info.ctime;
     st.st_size = info.size;
     printf("   getattr -> %llu %lu %lu %lu\n", info.size, info.atime, info.mtime, info.ctime);
   } else {
     yfs_client::dirinfo info;
     ret = yfs->getdir(inum, info);
     if(ret != yfs_client::OK)
       return ret;
     st.st_mode = S_IFDIR | 0777;
     st.st_nlink = 2;
     st.st_atime = info.atime;
     st.st_mtime = info.mtime;
     st.st_ctime = info.ctime;
     printf("   getattr -> %lu %lu %lu\n", info.atime, info.mtime, info.ctime);
   }
   return yfs_client::OK;
}


void
fuseserver_getattr(fuse_req_t req, fuse_ino_t ino,
          struct fuse_file_info *fi)
{
    struct stat st;
    yfs_client::inum inum = ino; // req->in.h.nodeid;
    yfs_client::status ret;

    ret = getattr(inum, st);
    if(ret != yfs_client::OK){
      fuse_reply_err(req, ENOENT);
      return;
    }
    fuse_reply_attr(req, &st, 0);
}

void
fuseserver_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr, int to_set, struct fuse_file_info *fi)
{
  printf("fuseserver_setattr 0x%x\n", to_set);
  if (FUSE_SET_ATTR_SIZE & to_set) {
    printf("   fuseserver_setattr set size to %llu\n", attr->st_size);
    struct stat st;
    // You fill this in
    yfs->setfilesize(ino, attr->st_size);
    if (getattr(ino, st) != yfs_client::OK) {
        fuse_reply_err(req, EIO);
        return;
    }
    getattr(ino, st);
    fuse_reply_attr(req, &st, 0);
  } else {
    fuse_reply_err(req, EIO);
  }
}

void
fuseserver_read(fuse_req_t req, fuse_ino_t ino, size_t size,
      off_t off, struct fuse_file_info *fi)
{
    printf("read ino=%llx, size=%u, off=%ld\n", ino, size, off);
  // You fill this in
    if (!yfs->isfile(ino)) {
        fuse_reply_err(req, EISDIR);
        return;
    }

    std::string buf;
    int r = yfs->get(ino, buf);
    if (r != yfs_client::OK) {
        int err = EIO;
        if (r == yfs_client::NOENT) {
            err = ENOENT;
        }
        fuse_reply_err(req, err);
        return;
    }

  fuse_reply_buf(req, buf.data() + off, size);
}

void
fuseserver_write(fuse_req_t req, fuse_ino_t ino,
  const char *buf, size_t size, off_t off,
  struct fuse_file_info *fi)
{
    printf("write ino=%llx, size=%u, off=%ld, flag=%08x\n", ino, size, off, fi->flags);
  // You fill this in
    if (!yfs->isfile(ino)) {
        fuse_reply_err(req, EISDIR);
        return;
    }
    int r;
    if (off == 0) {
        r = yfs->overwrite(ino, std::string(buf, size));
    } else {
        r = yfs->put(ino, std::string(buf, size), off);
    }
    if (r != yfs_client::OK) {
        int err = EIO;
        if (r == yfs_client::NOENT) {
            err = ENOENT;
        }
        fuse_reply_err(req, err);
        return;
    }

  fuse_reply_write(req, size);
}

fuse_ino_t geninum(bool file)
{
    unsigned int rand = random() & ((1<<31)-1);
    if (file) {
        rand = 0x80000000 | rand;
    }

    fuse_ino_t ino = rand;
    //fuse_ino_t ino = ((fuse_ino_t)myid) << 32;
    //ino |= rand;
    return ino;
}

static yfs_client::status
fuseserver_createhelper(fuse_ino_t parent, const char *name,
     mode_t mode, struct fuse_entry_param *e)
{
  // You fill this in
    std::string buf;
    fuse_ino_t id;
    int r;

    yfs->lock(parent);
    r = fuseserver_lookup_helper(parent, name, id);
    if (r == yfs_client::NOENT) {
        id = geninum(true);
        printf("create------->%016llx, %016llx, %s\n", parent, id, name);
        r = yfs->overwrite(id, "");
        if (r != yfs_client::OK) {
            goto done;
        }

        buf.append((char*) (&id), sizeof(fuse_ino_t));
        buf.append(name);
        buf.append("\n");
        r = yfs->append(parent, buf);
        if (r != yfs_client::OK) {
            goto done;
        }
    } else if (r == yfs_client::IOERR) {
        printf("lookup helper error[%d]------>%016llx, %s\n", r, parent, name);
        goto done;
    } else {
        printf("already exist file------>%016llx, %016llx, %s\n", parent, id, name);
    }

    e->ino = id;
    getattr(e->ino, e->attr);

done:
    yfs->unlock(parent);
    return r;
}

void
fuseserver_create(fuse_req_t req, fuse_ino_t parent, const char *name,
   mode_t mode, struct fuse_file_info *fi)
{
  struct fuse_entry_param e;

  if( fuseserver_createhelper( parent, name, mode, &e ) == yfs_client::OK ) {
    fuse_reply_create(req, &e, fi);
  } else {
    fuse_reply_err(req, ENOENT);
  }
}

void fuseserver_mknod( fuse_req_t req, fuse_ino_t parent, 
    const char *name, mode_t mode, dev_t rdev ) {
  struct fuse_entry_param e;
  if( fuseserver_createhelper( parent, name, mode, &e ) == yfs_client::OK ) {
    fuse_reply_entry(req, &e);
  } else {
    fuse_reply_err(req, ENOENT);
  }
}

static yfs_client::status
fuseserver_lookup_helper(fuse_ino_t parent, const char *name, fuse_ino_t &id)
{
  std::string buf;
  int r = yfs->get(parent, buf);
  if (r != yfs_client::OK) {
      return yfs_client::IOERR;
   }

  int i = sizeof(fuse_ino_t);
  int start = i;
  const char *raw = buf.c_str();
  while (i < buf.size()) {
      if (raw[i] == '\n') {
          if (i - start == strlen(name) &&
              strncmp(name, raw + start, strlen(name)) == 0) {
              memcpy(&id, raw + start - sizeof(fuse_ino_t), sizeof(fuse_ino_t));
              return yfs_client::OK;
          }
          i += sizeof(fuse_ino_t) + 1;
          start = i;
      } else {
          i++;
      }
  }

  return yfs_client::NOENT;
}

void
fuseserver_lookup(fuse_req_t req, fuse_ino_t parent, const char *name)
{
  struct fuse_entry_param e;
  bool found = false;

  printf("lookup------->%llx, %s\n", parent, name);

  // Look up the file named `name' in the directory referred to by
  // `parent' in YFS. If the file was found, initialize e.ino and
  // e.attr appropriately.

  fuse_ino_t id;
  yfs_client::status r = fuseserver_lookup_helper(parent, name, id);
  if (r != yfs_client::OK) {
      fuse_reply_err(req, ENOENT);
      return;
  }

  e.attr_timeout = 0.0;
  e.entry_timeout = 0.0;
  e.ino = id;
  getattr(e.ino, e.attr);

  fuse_reply_entry(req, &e);
}


struct dirbuf {
    char *p;
    size_t size;
};

void dirbuf_add(struct dirbuf *b, const char *name, fuse_ino_t ino)
{
    struct stat stbuf;
    size_t oldsize = b->size;
    b->size += fuse_dirent_size(strlen(name));
    b->p = (char *) realloc(b->p, b->size);
    memset(&stbuf, 0, sizeof(stbuf));
    stbuf.st_ino = ino;
    fuse_add_dirent(b->p + oldsize, name, &stbuf, b->size);
}

#define min(x, y) ((x) < (y) ? (x) : (y))

int reply_buf_limited(fuse_req_t req, const char *buf, size_t bufsize,
          off_t off, size_t maxsize)
{
  if (off < bufsize)
    return fuse_reply_buf(req, buf + off, min(bufsize - off, maxsize));
  else
    return fuse_reply_buf(req, NULL, 0);
}

void
fuseserver_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
          off_t off, struct fuse_file_info *fi)
{
    yfs_client::inum inum = ino; // req->in.h.nodeid;
    struct dirbuf b;
    yfs_client::dirent e;

    if (!yfs->isdir(inum)) {
        fuse_reply_err(req, ENOTDIR);
        return;
    }

    memset(&b, 0, sizeof(b));

    int r;
    std::string buf = "";
    r = yfs->get(inum, buf);
    if (r != yfs_client::OK) {
        printf("readdir(get file)error: %d\n", r);
        if (r == yfs_client::NOENT) {
            fuse_reply_err(req, ENOENT);
        }
        fuse_reply_err(req, EIO);
        return;
    }

    // fill in the b data structure using dirbuf_add

    int start = 0;
    int i = start + sizeof(fuse_ino_t), j = 0;
    fuse_ino_t dir_ino;
    char name[1024];
    const char *raw = buf.c_str();
    while (i < buf.size()) {
        if (raw[i] == '\n') {
            name[j++] = '\0';
            memcpy(&dir_ino, raw + start, sizeof(fuse_ino_t));
            start = i + 1;
            i = start + sizeof(fuse_ino_t);
            j = 0;
            dirbuf_add(&b, name, dir_ino);
        } else {
            name[j++] = raw[i++];
        }
    }

    reply_buf_limited(req, b.p, b.size, off, size);
    free(b.p);
 }


void
fuseserver_open(fuse_req_t req, fuse_ino_t ino,
     struct fuse_file_info *fi)
{
  // You fill this in
    int err = EIO, r;

    if (!yfs->isfile(ino)) {
        err = EISDIR;
        goto error;
    }

    fuse_reply_open(req, fi);
    return;

error:
    fuse_reply_err(req, err);
}

void
fuseserver_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name,
     mode_t mode)
{
    struct fuse_entry_param e;
    std::string buf;
    fuse_ino_t id;
    int err, r;

    // You fill this in
    yfs->lock(parent);
    r = fuseserver_lookup_helper(parent, name, id);
    if (r == yfs_client::NOENT) {
        id = geninum(false);
        printf("mkdir------->%016llx, %016llx, %s\n", parent, id, name);
        r = yfs->overwrite(id, "");
        if (r != yfs_client::OK) {
            err = EIO;
            goto error;
        }

        buf.append((char*) (&id), sizeof(fuse_ino_t));
        buf.append(name);
        buf.append("\n");
        r = yfs->append(parent, buf);
        if (r != yfs_client::OK) {
            err = EIO;
            goto error;
        }
    } else if (r == yfs_client::IOERR) {
        printf("lookup helper error[%d]------>%016llx, %s\n", r, parent, name);
        err = ENOENT;
        goto error;
    } else {
        printf("already exist dir------->%016llx, %016llx, %s\n", parent, id, name);
    }

    e.ino = id;
    getattr(e.ino, e.attr);

    yfs->unlock(parent);
    fuse_reply_entry(req, &e);
    return;

error:
    yfs->unlock(parent);
    fuse_reply_err(req, err);
}

void
fuseserver_unlink(fuse_req_t req, fuse_ino_t parent, const char *name)
{
  // You fill this in
  // Success:	fuse_reply_err(req, 0);
  // Not found:	fuse_reply_err(req, ENOENT);
    yfs->lock(parent);
    int r = yfs->remove(parent, std::string(name));
    yfs->unlock(parent);
    if (r != yfs_client::OK) {
        fuse_reply_err(req, ENOENT);
    } else {
        fuse_reply_err(req, 0);
    }
}

void
fuseserver_statfs(fuse_req_t req)
{
  struct statvfs buf;

  printf("statfs\n");

  memset(&buf, 0, sizeof(buf));

  buf.f_namemax = 255;
  buf.f_bsize = 512;

  fuse_reply_statfs(req, &buf);
}

struct fuse_lowlevel_ops fuseserver_oper;

int
main(int argc, char *argv[])
{
  char *mountpoint = 0;
  int err = -1;
  int fd;

  setvbuf(stdout, NULL, _IONBF, 0);

  if(argc != 4){
    fprintf(stderr, "Usage: yfs_client <mountpoint> <port-extent-server> <port-lock-server>\n");
    exit(1);
  }
  mountpoint = argv[1];

  srandom(getpid());

  myid = random();

  yfs = new yfs_client(argv[2], argv[3]);


  fuseserver_oper.getattr    = fuseserver_getattr;
  fuseserver_oper.statfs     = fuseserver_statfs;
  fuseserver_oper.readdir    = fuseserver_readdir;
  fuseserver_oper.lookup     = fuseserver_lookup;
  fuseserver_oper.create     = fuseserver_create;
  fuseserver_oper.mknod      = fuseserver_mknod;
  fuseserver_oper.open       = fuseserver_open;
  fuseserver_oper.read       = fuseserver_read;
  fuseserver_oper.write      = fuseserver_write;
  fuseserver_oper.setattr    = fuseserver_setattr;
  fuseserver_oper.unlink     = fuseserver_unlink;
  fuseserver_oper.mkdir      = fuseserver_mkdir;

  const char *fuse_argv[20];
  int fuse_argc = 0;
  fuse_argv[fuse_argc++] = argv[0];
#ifdef __APPLE__
  fuse_argv[fuse_argc++] = "-o";
  fuse_argv[fuse_argc++] = "nolocalcaches"; // no dir entry caching
  fuse_argv[fuse_argc++] = "-o";
  fuse_argv[fuse_argc++] = "daemon_timeout=86400";
#endif

  // everyone can play, why not?
  //fuse_argv[fuse_argc++] = "-o";
  //fuse_argv[fuse_argc++] = "allow_other";

  fuse_argv[fuse_argc++] = mountpoint;
  fuse_argv[fuse_argc++] = "-d";

  fuse_args args = FUSE_ARGS_INIT( fuse_argc, (char **) fuse_argv );
  int foreground;
  int res = fuse_parse_cmdline( &args, &mountpoint, 0 /*multithreaded*/, 
        &foreground );
  if( res == -1 ) {
    fprintf(stderr, "fuse_parse_cmdline failed\n");
    return 0;
  }
  
  args.allocated = 0;

  yfs->overwrite(1, "");

  fd = fuse_mount(mountpoint, &args);
  if(fd == -1){
    fprintf(stderr, "fuse_mount failed\n");
    exit(1);
  }

  struct fuse_session *se;

  se = fuse_lowlevel_new(&args, &fuseserver_oper, sizeof(fuseserver_oper),
       NULL);
  if(se == 0){
    fprintf(stderr, "fuse_lowlevel_new failed\n");
    exit(1);
  }

  struct fuse_chan *ch = fuse_kern_chan_new(fd);
  if (ch == NULL) {
    fprintf(stderr, "fuse_kern_chan_new failed\n");
    exit(1);
  }

  fuse_session_add_chan(se, ch);

  // err = fuse_session_loop_mt(se);   // FK: wheelfs does this; why?
  err = fuse_session_loop(se);
    
  fuse_session_destroy(se);
  close(fd);
  fuse_unmount(mountpoint);

  return err ? 1 : 0;
}
