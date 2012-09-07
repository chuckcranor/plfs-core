#ifndef _IOSTORE_H_
#define _IOSTORE_H_

#include <fcntl.h>
#include <unistd.h>
#include <utime.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>

class IOStore;
class IOSHandle;
class IOSDirHandle;

/**
 * IOStore: A pure virtual class for IO manipulation of a backend store
 */
class IOStore {
 public:
    virtual int Access(const char *bpath, int mode)=0;
    virtual int Chown(const char *bpath, uid_t owner, gid_t group)=0;
    virtual int Chmod(const char *bpath, mode_t mode)=0;
    int Close(IOSHandle *handle);               /* inlined below */
    int Closedir(class IOSDirHandle *dhandle);  /* inlined below */
    virtual int Lchown(const char *bpath, uid_t owner, gid_t group)=0;
    virtual int Link(const char *bpath1, const char *bpath2)=0;
    virtual int Lstat(const char *bpath, struct stat *sb)=0;
    virtual int Mkdir(const char *bpath, mode_t mode)=0;
    virtual int Mknod(const char *bpath, mode_t mode, dev_t dev)=0;
    /* Chuck, this open takes args that are very POSIX specific */
    virtual class IOSHandle *Open(const char *bpath, int flags, mode_t mode)=0;
    virtual IOSDirHandle *Opendir(const char *bpath)=0;
    virtual int Rename(const char *frombpath, const char *tobpath)=0;
    virtual int Rmdir(const char *bpath)=0;
    virtual int Stat(const char *bpath, struct stat *sb)=0;
    virtual int Statvfs( const char *path, struct statvfs* stbuf )=0;
    virtual int Symlink(const char *bpath1, const char *bpath2)=0;
    virtual ssize_t Readlink(const char *bpath, char *buf, size_t bufsize)=0;
    virtual int Truncate (const char *bpath, off_t length)=0;
    virtual int Unlink(const char *bpath)=0;
    virtual int Utime(const char *bpath, const struct utimbuf *times)=0;
    virtual ~IOStore() { }

    /* two simple compat APIs that can be inlined by the compiler */
    class IOSHandle *Creat(const char *bpath, mode_t mode) {
        return(Open(bpath, O_CREAT|O_TRUNC|O_WRONLY, mode));
    };
    class IOSHandle *Open(const char *bpath, int flags) {
        return(Open(bpath, flags, 0777));
    };
};

/**
 * IOSHandle: iostore open file handle.  this is the iostore version
 * of the posix int file descriptor.  all functions that operation on
 * file descriptors belong here.
 */
class IOSHandle {
 private:
    virtual int Close(void)=0;
    friend int IOStore::Close(IOSHandle *handle);
    virtual int Open(int flags, mode_t mode); 
    
 public:
    virtual int Fstat(struct stat *sb)=0;
    virtual int Fsync(void)=0;
    virtual int Ftruncate(off_t length)=0;
    virtual off_t Lseek(off_t offset, int whence)=0;

    // XXX: PLFS only uses this for memory mapping a file read-only to
    // avoid a data copy, so this is a richer API than needed (and it
    // doesn't have to be a memory map in this case, a malloc/read/free
    // sequence works too).  since memory mapped isn't going to work
    // on non-kernel backends (e.g. HDFS, IOFSL) we had better keep it
    // optional (or easy to emuluate without real mmap).
    virtual void *Mmap(void *addr, size_t len, int prot,
                       int flags, off_t offset)=0;

    // XXX: Munmap doesn't operate on a file handle, so maybe it doesn't
    // belong here, but it pairs well with Mmap so we'll keep it here
    // for now.
    virtual int Munmap(void *addr, size_t length)=0;

    virtual ssize_t Pread(void *buf, size_t nbytes, off_t offset)=0;
    virtual ssize_t Pwrite(const void *buf, size_t nbytes, off_t offset)=0;
    virtual ssize_t Read(void *buf, size_t offset)=0;
    virtual ssize_t Write(const void *buf , size_t nbytes)=0;
};

/**
 * IOSDirHandle: iostore open directory handle.   this is the iostore
 * version of a DIR*.
 */
class IOSDirHandle {
 private:
    virtual int Closedir(void)=0;
    friend int IOStore::Closedir(IOSDirHandle *handle);
    
public:
    virtual int Readdir_r(struct dirent *, struct dirent **)=0;
};

/*
 * wrapper IOStore close APIs that lock the handle close op with the
 * delete op (compiler can inline this).   these need both the IOStore
 * and IOSHandle classes to be defined first, so they have to be
 * down here.
 */
inline int IOStore::Close(IOSHandle *handle) {
    int rv;
    rv = handle->Close();
    delete handle;
    return(rv);
};

inline int IOStore::Closedir(IOSDirHandle *handle) {
    int rv;
    rv = handle->Closedir();
    delete handle;
    return(rv);
};

#endif
