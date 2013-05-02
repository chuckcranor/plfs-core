/*
 * ContainerFD.cpp  logical file descriptor interface for container mode
 */

#include "plfs.h"
#include "plfs_private.h"
#include "ContainerFS.h"
#include "ContainerFD.h"
#include "container_internals.h"
#include "XAttrs.h"
#include "mlog.h"
#include "mlogfacs.h"
#include "mlog_oss.h"
#include <assert.h> /* XXXCDC */

// TODO:
// this global variable should be a plfs conf
// do we try to cache a read index even in RDWR mode?
// if we do, blow it away on writes
// otherwise, blow it away whenever it gets created
// it would be nice to change this to true but it breaks something
// figure out what and change.  do not change to true without figuring out
// how it breaks things.  It should be obvious.  Try to build PLFS inside
// PLFS and it will break.
bool cache_index_on_rdwr = false;   // DO NOT change to true!!!!

/*
 * helper functions
 */
// this code is where the magic lives to get the distributed hashing
// each proc just tries to create their data and index files in the
// canonical_container/hostdir but if that hostdir doesn't exist,
// then the proc creates a shadow_container/hostdir and links that
// into the canonical_container
// returns number of current writers sharing the WriteFile * or -err
static int
addPrepareWriter( WriteFile *wf, pid_t pid, mode_t mode,
                  const string& logical, bool for_open, bool defer_open )
{
    int ret, writers;

    // might have to loop 3 times
    // first discover that the subdir doesn't exist
    // try to create it and try again
    // if we fail to create it bec someone else created a metalink there
    // then try again into where the metalink resolves
    // but that might fail if our sibling hasn't created where it resolves yet
    // so help our sibling create it, and then finally try the third time.
    for( int attempts = 0; attempts < 2; attempts++ ) {
        // for defer_open , wf->addWriter() only increases writer ref counts,
        // since it doesn't actually do anything until it gets asked to write
        // for the first time at which point it actually then attempts to
        // O_CREAT its required data and index logs
        // for !defer_open, the WriteFile *wf has a container path in it
        // which is path to canonical.  It attempts to open a file in a subdir
        // at that path.  If it fails, it should be bec there is no
        // subdir in the canonical. [If it fails for any other reason, something
        // is badly broken somewhere.]
        // When it fails, create the hostdir.  It might be a metalink in
        // which case change the container path in the WriteFile to shadow path
        ret = wf->addWriter( pid, for_open, defer_open, writers );
        if ( ret != -ENOENT ) {
            break;    // everything except ENOENT leaves
        }
        // if we get here, the hostdir doesn't exist (we got ENOENT)
        // here is a super simple place to add the distributed metadata stuff.
        // 1) create a shadow container by hashing on node name
        // 2) create a shadow hostdir inside it
        // 3) create a metalink in canonical container identifying shadow
        // 4) change the WriteFile path to point to shadow
        // 4) loop and try one more time
        string physical_hostdir;
        bool use_metalink = false;
        // discover all physical paths from logical one
        ContainerPaths paths;
        ret = Container::findContainerPaths(logical,paths);
        if (ret!=0) {
            PLFS_EXIT(ret);
        }
        struct plfs_backend *newback;
        ret=Container::makeHostDir(paths, mode, PARENT_ABSENT,
                                   physical_hostdir, &newback, use_metalink);
        if ( ret==0 ) {
            // a sibling raced us and made the directory or link for us
            // or we did
            wf->setSubdirPath(physical_hostdir, newback);
            if (!use_metalink) {
                wf->setContainerPath(paths.canonical);
            } else {
                wf->setContainerPath(paths.shadow);
            }
        } else {
            mlog(INT_DRARE,"Something weird in %s for %s.  Retrying.",
                 __FUNCTION__, paths.shadow.c_str());
            continue;
        }
    }
    // all done.  we return either -err or number of writers.
    if ( ret == 0 ) {
        ret = writers;
    }
    PLFS_EXIT(ret);
}


static int
isWriter( int flags )
{
    return (flags & O_WRONLY || flags & O_RDWR );
}

// Was running into reference count problems so I had to change this code
// The RDONLY flag is has the lsb set as 0 had to do some bit shifting
// to figure out if the RDONLY flag was set
static int
isReader( int flags )
{
    int ret = 0;
    if ( flags & O_RDWR ) {
        ret = 1;
    } else {
        unsigned int flag_test = (flags << ((sizeof(int)*8)-2));
        if ( flag_test == 0 ) {
            ret = 1;
        }
    }
    return ret;
}

int
openAddWriter( WriteFile *wf, pid_t pid, mode_t mode, string logical,
               bool defer_open )
{
    return addPrepareWriter( wf, pid, mode, logical, true, defer_open );
}


// TODO: rename to container_reference_count
static ssize_t
plfs_reference_count( Container_OpenFile *pfd )
{
    WriteFile *wf = pfd->getWritefile();
    Index     *in = pfd->getIndex();
    int ref_count = 0;
    if ( wf ) {
        ref_count += wf->numWriters();
    }
    if ( in ) {
        ref_count += in->incrementOpens(0);
    }
    if ( ref_count != pfd->incrementOpens(0) ) {
        mlog(INT_DRARE, "%s not equal counts: %d != %d",
             __FUNCTION__, ref_count, pfd->incrementOpens(0));
        assert( ref_count == pfd->incrementOpens(0) );
    }
    return ref_count;
}

Container_fd::Container_fd()
{
    fd = NULL;
}

Container_fd::~Container_fd()
{
    return;
}

// pass in a NULL Container_OpenFile to have one created for you
// pass in a valid one to add more writers to it
// one problem is that we fail if we're asked to overwrite a normal file
// in RDWR mode, we increment reference count twice.  make sure to decrement
// twice on the close
int
Container_fd::open(const char *logical, int flags, pid_t pid,
                   mode_t mode, Plfs_open_opt *open_opt)
{
    PLFS_ENTER;
    Container_OpenFile **pfd = &fd;  /* XXXCDC reorg */
    WriteFile *wf      = NULL;
    Index     *index   = NULL;
    bool new_writefile = false;
    bool new_index     = false;
    /*
    if ( pid == 0 && open_opt && open_opt->pinter == PLFS_MPIIO ) {
        // just one message per MPI open to make sure the version is right
        fprintf(stderr, "PLFS version %s\n", plfs_version());
    }
    */
    // ugh, no idea why this line is here or what it does
    if ( mode == 420 || mode == 416 ) {
        mode = 33152;
    }
    // make sure we're allowed to open this container
    // this breaks things when tar is trying to create new files
    // with --r--r--r bec we create it w/ that access and then
    // we can't write to it
    //ret = Container::Access(path.c_str(),flags);
    if ( ret == 0 && flags & O_CREAT ) {
        ret = containerfs.create( logical, mode, flags, pid );
    }
    if ( ret == 0 && flags & O_TRUNC ) {
        // truncating an open file
        ret = container_trunc( NULL, logical, 0,(int)true );
    }
    if ( ret == 0 && *pfd) {
        plfs_reference_count(*pfd);
    }
    // this next chunk of code works similarly for writes and reads
    // for writes, create a writefile if needed, otherwise add a new writer
    // create the write index file after the write data file so that the
    // hostdir is already created
    // for reads, create an index if needed, otherwise add a new reader
    // this is so that any permission errors are returned on open
    if ( ret == 0 && isWriter(flags) ) {
        if ( *pfd ) {
            wf = (*pfd)->getWritefile();
        }
        if ( wf == NULL ) {
            // do we delete this on error?
            size_t indx_sz = 0;
            if(open_opt&&open_opt->pinter==PLFS_MPIIO &&
                    open_opt->buffer_index) {
                // this means we want to flatten on close
                indx_sz = get_plfs_conf()->buffer_mbs;
            }
            /*
             * wf starts with the canonical backend.   the openAddWriter()
             * call below may change it (e.g. to a shadow backend).
             */
            wf = new WriteFile(path, Util::hostname(), mode,
                               indx_sz, pid, logical, expansion_info.backend);
            new_writefile = true;
        }
        bool defer_open = get_plfs_conf()->lazy_droppings;
        ret = openAddWriter(wf, pid, mode, logical, defer_open );
        mlog(INT_DCOMMON, "%s added writer: %d", __FUNCTION__, ret );
        if ( ret > 0 ) {
            ret = 0;    // add writer returns # of current writers
        }
        if ( ret == 0 && new_writefile && !defer_open ) {
            ret = wf->openIndex( pid );
        }
        if ( ret != 0 && wf ) {
            delete wf;
            wf = NULL;
        }
    }
    if ( ret == 0 && isReader(flags)) {
        if ( *pfd ) {
            index = (*pfd)->getIndex();
        }
        if ( index == NULL ) {
            // do we delete this on error?
            index = new Index( path, expansion_info.backend );
            new_index = true;
            // Did someone pass in an already populated index stream?
            if (open_opt && open_opt->index_stream !=NULL) {
                //Convert the index stream to a global index
                index->global_from_stream(open_opt->index_stream);
            } else {
                ret = Container::populateIndex(path,expansion_info.backend,
                   index,true,
                   open_opt ? open_opt->uniform_restart_enable : 0,
                   open_opt ? open_opt->uniform_restart_rank : 0 );
                if ( ret != 0 ) {
                    mlog(INT_DRARE, "%s failed to create index on %s: %s",
                         __FUNCTION__, path.c_str(), strerror(-ret));
                    delete(index);
                    index = NULL;
                }
            }
        }
        if ( ret == 0 ) {
            index->incrementOpens(1);
        }
        // can't cache index if error or if in O_RDWR
        // be nice to be able to cache but trying to do so
        // breaks things.  someone should fix this one day
        if (index) {
            bool delete_index = false;
            if (ret!=0) {
                delete_index = true;
            }
            if (!cache_index_on_rdwr && isWriter(flags)) {
                delete_index = true;
            }
            if (delete_index) {
                delete index;
                index = NULL;
            }
        }
    }
    if ( ret == 0 && ! *pfd ) {
        // do we delete this on error?
        *pfd = new Container_OpenFile( wf, index, pid, mode,
                                       path.c_str(), expansion_info.backend );
        // we create one open record for all the pids using a file
        // only create the open record for files opened for writing
        if ( wf ) {
            bool add_meta = true;
            if (open_opt && open_opt->pinter==PLFS_MPIIO && pid != 0 ) {
                add_meta = false;
            }
            if (add_meta) {
                ret = Container::addOpenrecord(path, expansion_info.backend,
                                               Util::hostname(),pid);
            }
        }
        //cerr << __FUNCTION__ << " added open record for " << path << endl;
    } else if ( ret == 0 ) {
        if ( wf && new_writefile) {
            (*pfd)->setWritefile( wf );
        }
        if ( index && new_index ) {
            (*pfd)->setIndex(index);
        }
    }
    if (ret == 0) {
        // do we need to incrementOpens twice if O_RDWR ?
        // if so, we need to decrement twice in close
        if (wf && isWriter(flags)) {
            (*pfd)->incrementOpens(1);
        }
        if(index && isReader(flags)) {
            (*pfd)->incrementOpens(1);
        }
        plfs_reference_count(*pfd);
        if (open_opt && open_opt->reopen==1) {
            (*pfd)->setReopen();
        }
    }
    PLFS_EXIT(ret);
}

// returns number of open handles or -err
// the close_opt currently just means we're in ADIO mode
int
Container_fd::close(pid_t pid, uid_t uid, int open_flags,
                    Plfs_close_opt *close_opt)
{
    Container_OpenFile *pfd = this->fd; /*XXXCDC: reorg */
    int ret = 0;
    WriteFile *wf    = pfd->getWritefile();
    Index     *index = pfd->getIndex();
    size_t writers = 0, readers = 0, ref_count = 0;
    // be careful.  We might enter here when we have both writers and readers
    // make sure to remove the appropriate open handle for this thread by
    // using the original open_flags
    // clean up after writes
    if ( isWriter(open_flags) ) {
        assert(wf);
        writers = wf->removeWriter( pid );
        if ( writers == 0 ) {
            off_t  last_offset;
            size_t total_bytes;
            bool drop_meta = true; // in ADIO, only 0; else, everyone
            if(close_opt && close_opt->pinter==PLFS_MPIIO) {
                if (pid==0) {
                    if(close_opt->valid_meta) {
                        mlog(PLFS_DCOMMON, "Grab meta from ADIO gathered info");
                        last_offset=close_opt->last_offset;
                        total_bytes=close_opt->total_bytes;
                    } else {
                        mlog(PLFS_DCOMMON, "Grab info from glob merged idx");
                        last_offset=index->lastOffset();
                        total_bytes=index->totalBytes();
                    }
                } else {
                    drop_meta = false;
                }
            } else {
                wf->getMeta( &last_offset, &total_bytes );
            }
            if ( drop_meta ) {
                size_t max_writers = wf->maxWriters();
                if (close_opt && close_opt->num_procs > max_writers) {
                    max_writers = close_opt->num_procs;
                }
                Container::addMeta(last_offset, total_bytes, pfd->getPath(),
                                   pfd->getCanBack(),
                                   Util::hostname(),uid,wf->createTime(),
                                   close_opt?close_opt->pinter:-1,
                                   max_writers);
                Container::removeOpenrecord( pfd->getPath(), pfd->getCanBack(),
                                             Util::hostname(),
                                             pfd->getPid());
            }
            // the pfd remembers the first pid added which happens to be the
            // one we used to create the open-record
            delete wf;
            wf = NULL;
            pfd->setWritefile(NULL);
        } else {
            ret = 0;
        }
        ref_count = pfd->incrementOpens(-1);
        // Clean up reads moved fd reference count updates
    }
    if (isReader(open_flags) && index) {
        assert( index );
        readers = index->incrementOpens(-1);
        if ( readers == 0 ) {
            delete index;
            index = NULL;
            pfd->setIndex(NULL);
        }
        ref_count = pfd->incrementOpens(-1);
    }
    mlog(PLFS_DCOMMON, "%s %s: %d readers, %d writers, %d refs remaining",
         __FUNCTION__, pfd->getPath(), (int)readers, (int)writers,
         (int)ref_count);
    // make sure the reference counting is correct
    plfs_reference_count(pfd);
    if ( ret == 0 && ref_count == 0 ) {
        mss::mlog_oss oss(PLFS_DCOMMON);
        oss << __FUNCTION__ << " removing OpenFile " << pfd;
        oss.commit();
        delete pfd;
        pfd = NULL;
    }
    return ( ret < 0 ? ret : ref_count );
}

// returns -errno or bytes read
ssize_t
Container_fd::read(char *buf, size_t size, off_t offset)
{
    Container_OpenFile *pfd = this->fd;  /* XXXCDC: reorg */
    bool new_index_created = false;
    Index *index = pfd->getIndex();
    ssize_t ret = 0;
    mlog(PLFS_DAPI, "Read request on %s at offset %ld for %ld bytes",
         pfd->getPath(),long(offset),long(size));
    // possible that we opened the file as O_RDWR
    // if so, we may not have a persistent index
    // build an index now, but destroy it after this IO
    // so that new writes are re-indexed for new reads
    // basically O_RDWR is possible but it can reduce read BW
    if (index == NULL) {
        index = new Index(pfd->getPath(), pfd->getCanBack());
        if ( index ) {
            // if they tried to do uniform restart, it will only work at open
            // uniform restart doesn't currently work with O_RDWR
            // to make it work, we'll have to store the uniform restart info
            // into the Container_OpenFile
            new_index_created = true;
            ret = Container::populateIndex(pfd->getPath(),pfd->getCanBack(),
                                           index,false,false,0);
        } else {
            ret = -EIO;
        }
    }
    if ( ret == 0 ) {
        ret = plfs_reader(pfd,buf,size,offset,index);
    }
    mlog(PLFS_DAPI, "Read request on %s at offset %ld for %ld bytes: ret %ld",
         pfd->getPath(),long(offset),long(size),long(ret));
    // we created a new index.  Maybe we cache it or maybe we destroy it.
    if (new_index_created) {
        bool delete_index = true;
        if (cache_index_on_rdwr) {
            pfd->lockIndex();
            if (pfd->getIndex()==NULL) { // no-one else cached one
                pfd->setIndex(index);
                delete_index = false;
            }
            pfd->unlockIndex();
        }
        if (delete_index) {
            delete(index);
        }
        mlog(PLFS_DCOMMON, "%s %s freshly created index for %s",
             __FUNCTION__, delete_index?"removing":"caching", pfd->getPath());
    }
    PLFS_EXIT(ret);
}

// this function is important because when an open file is renamed
// we need to know about it bec when the file is closed we need
// to know the correct phyiscal path to the container in order to
// create the meta dropping
int
Container_fd::rename(const char *logical, struct plfs_backend *b) {
    PLFS_ENTER;
    this->fd->setPath(path.c_str(),b);
    WriteFile *wf = this->fd->getWritefile();
    if ( wf )
        wf->setLogical(logical);
    PLFS_EXIT(ret);
}

ssize_t
Container_fd::write(const char *buf, size_t size, off_t offset, pid_t pid)
{
    Container_OpenFile *pfd = this->fd;
    // this can fail because this call is not in a mutex so it's possible
    // that some other thread in a close is changing ref counts right now
    // but it's OK that the reference count is off here since the only
    // way that it could be off is if someone else removes their handle,
    // but no-one can remove the handle being used here except this thread
    // which can't remove it now since it's using it now
    //plfs_reference_count(pfd);
    // possible that we cache index in RDWR.  If so, delete it on a write
    /*
    Index *index = pfd->getIndex();
    if (index != NULL) {
        assert(cache_index_on_rdwr);
        pfd->lockIndex();
        if (pfd->getIndex()) { // make sure another thread didn't delete
            delete index;
            pfd->setIndex(NULL);
        }
        pfd->unlockIndex();
    }
    */
    int ret = 0;
    ssize_t written;
    WriteFile *wf = pfd->getWritefile();
    ret = written = wf->write(buf, size, offset, pid);
    mlog(PLFS_DAPI, "%s: Wrote to %s, offset %ld, size %ld: ret %ld",
         __FUNCTION__, pfd->getPath(), (long)offset, (long)size, (long)ret);
    PLFS_EXIT( ret >= 0 ? written : ret );
}

int
Container_fd::sync()
{
    return ( this->fd->getWritefile() ? 
             this->fd->getWritefile()->sync() : 0 );
}

int
Container_fd::sync(pid_t pid)
{
    return ( this->fd->getWritefile() ? 
             this->fd->getWritefile()->sync(pid) : 0 );
}

int
Container_fd::trunc(const char *path, off_t offset)
{
    /* XXXCDC: reorg -- container trunc multiple refs */
    bool open_file = true; // Yes, I am an open file handle.
    return container_trunc(fd, path, offset, open_file);
}

// there's a lazy stat flag, sz_only, which means all the caller cares
// about is the size of the file.  If the file is currently
// open (i.e. we have a wf ptr, then the size info is stashed in
// there.  It might not be fully accurate since it just contains info
// for the writes of the current proc but it's a good-enough estimate
// however, if the caller hasn't passed lazy or if the wf isn't
// available then we need to do a more expensive descent into
// the container.  This descent is especially expensive for an open
// file where we can't just used the cached meta info but have to
// actually fully populate an index structure and query it
int
Container_fd::getattr(const char *unusedpath, struct stat *stbuf, int sz_only)
{
    int ret, im_lazy;
    string fdpath;
    struct plfs_backend *backend;
    WriteFile *wf;

    /* if it is an open file, then it has to be a container */
    fdpath = this->fd->getPath();
    backend = this->fd->getCanBack();
    wf = this->fd->getWritefile();

    im_lazy = (sz_only && wf && !this->fd->isReopen());
    
    mlog(PLFS_DAPI, "%s on open file %s (lazy=%d)", __FUNCTION__,
         fdpath.c_str(), im_lazy);
    memset(stbuf, 0, sizeof(*stbuf));   /* XXX: necessary? */
    
    if (im_lazy) {
        ret = 0;     /* successfully skipped the heavyweight getattr call */
    } else {
        ret = Container::getattr(fdpath, backend, stbuf);
    } 

    if (ret == 0 && wf != NULL) {
        off_t last_offset;
        size_t total_bytes;
        wf->getMeta(&last_offset, &total_bytes);
        mlog(PLFS_DCOMMON, "got meta from openfile: %lu last offset, "
             "%ld total bytes", (unsigned long)last_offset,
             (unsigned long)total_bytes);
        if (last_offset > stbuf->st_size) {
            stbuf->st_size = last_offset;
        }
        if (im_lazy) {
            stbuf->st_blocks = Container::bytesToBlocks(total_bytes);
        }
    }
    
    mlog(PLFS_DAPI, "%s: getattr(%s) size=%ld, ret=%s", __FUNCTION__,
         fdpath.c_str(), (unsigned long)stbuf->st_size,
         (ret == 0) ? "AOK" : strerror(-ret));
         
    return(ret);
}

// TODO: add comments.  what does this do?  why?  who might call it?
int
Container_fd::query(size_t *writers, size_t *readers, size_t *bytes_written,
                    bool *reopen)
{
    Container_OpenFile *pfd = this->fd; /*XXXCDC: reorg */
    WriteFile *wf = pfd->getWritefile();
    Index     *ix = pfd->getIndex();
    if (writers) {
        *writers = 0;
    }
    if (readers) {
        *readers = 0;
    }
    if (bytes_written) {
        *bytes_written = 0;
    }
    if ( wf && writers ) {
        *writers = wf->numWriters();
    }
    if ( ix && readers ) {
        *readers = ix->incrementOpens(0);
    }
    if ( wf && bytes_written ) {
        off_t  last_offset;
        size_t total_bytes;
        wf->getMeta( &last_offset, &total_bytes );
        mlog(PLFS_DCOMMON, "container_query Got meta from openfile: "
             "%lu last offset, "
             "%ld total bytes", (unsigned long)last_offset,
             (unsigned long)total_bytes);
        *bytes_written = total_bytes;
    }
    if (reopen) {
        *reopen = pfd->isReopen();
    }
    return 0;
}

bool
Container_fd::is_good()
{
    return true;
}

int
Container_fd::incrementOpens(int amount)
{
    return fd->incrementOpens(amount);
}

void
Container_fd::setPath(string p, struct plfs_backend *b)
{
    fd->setPath(p,b);
}

// should be called with a logical path and already_expanded false
// or called with a physical path and already_expanded true
// returns 0 or -err
int
Container_fd::compress_metadata(const char *logical)
{
    Container_OpenFile *pfd = this->fd;
    PLFS_ENTER;
    Index *index;
    bool newly_created = false;
    if ( pfd && pfd->getIndex() ) {
        index = pfd->getIndex();
    } else {
        index = new Index( path, expansion_info.backend );
        newly_created = true;
        // before we populate, need to blow away any old one
        ret = Container::populateIndex(path,expansion_info.backend,
                index,false,false,0);
        /* XXXCDC: why are we ignoring return value of populateIndex? */
    }
    if (is_container_file(logical,NULL)) {
        ret = Container::flattenIndex(path,expansion_info.backend,index);
    } else {
        ret = -EBADF; // not sure here.  Maybe return SUCCESS?
    }
    if (newly_created) {
        delete index;
    }
    PLFS_EXIT(ret);
}

const char *
Container_fd::getPath()
{
    return fd->getPath();
}

int
Container_fd::getxattr(void *value, const char *key, size_t len) {
    XAttrs *xattrs;
    XAttr *xattr;
    int ret = 0;

    xattrs = new XAttrs(getPath(), this->fd->getCanBack());
    xattr = xattrs->getXAttr(string(key), len);
    if (xattr == NULL) {
        ret = 1;
        return ret;
    }

    memcpy(value, xattr->getValue(), len);
    delete(xattr);
    delete(xattrs);

    return ret;
}

int
Container_fd::setxattr(const void *value, const char *key, size_t len) {
    stringstream sout;
    XAttrs *xattrs;
    bool xret;
    int ret = 0;

    mlog(PLFS_DBG, "In %s: Setting xattr - key: %s, value: %s\n", 
         __FUNCTION__, key, (char *)value);
    xattrs = new XAttrs(getPath(), this->fd->getCanBack());
    xret = xattrs->setXAttr(string(key), value, len);
    if (!xret) {
        mlog(PLFS_DBG, "In %s: Error writing upc object size\n", 
             __FUNCTION__);
        ret = 1;
    }

    delete(xattrs);

    return ret;
}
