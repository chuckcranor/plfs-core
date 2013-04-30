/*
 * ContainerFS.cpp  logical filesystem interface for container mode
 */

#include "plfs.h"
#include "plfs_private.h"
#include "ContainerFS.h"
#include "ContainerFD.h"
#include "container_internals.h"   /* XXXCDC */
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>

/*
 * containerfs: global object that containers all our functions, used
 * in plfs_private.cpp as the default logical filesystem and also
 * chosen if plfsrc has 'workload n-1' or 'workload shared_file'.
 */
ContainerFileSystem containerfs;

/*
 * private helper functions
 */



// takes a logical path for a logical file and returns every physical component
// comprising that file (canonical/shadow containers, subdirs, data files, etc)
// may not be efficient since it checks every backend and probably some backends
// won't exist.  Will be better to make this just go through canonical and find
// everything that way.
// returns 0 or -err
int
plfs_collect_from_containers(const char *logical, vector<plfs_pathback> &files,
                             vector<plfs_pathback> &dirs,
                             vector<plfs_pathback> &links)
{
    PLFS_ENTER;
    vector<plfs_pathback> possible_containers;
    ret = find_all_expansions(logical,possible_containers);
    if (ret!=0) {
        PLFS_EXIT(ret);
    }
    vector<plfs_pathback>::iterator itr;
    for(itr=possible_containers.begin();
            itr!=possible_containers.end();
            itr++) {
        ret = Util::traverseDirectoryTree(itr->bpath.c_str(), itr->back,
                                          files,dirs,links);
        if (ret < 0) {
            break;
        }
    }
    PLFS_EXIT(ret);
}

// this function is shared by chmod/utime/chown maybe others
// anything that needs to operate on possibly a lot of items
// either on a bunch of dirs across the backends
// or on a bunch of entries within a container
// Be careful.  This performs a stat.  Do not use
// for performance critical operations.  If needed,
// then you'll have to figure out how to cheaply pass
// the mode_t in
// returns 0 or -err
static int
plfs_file_operation(const char *logical, FileOp& op)
{
    PLFS_ENTER;
    vector<plfs_pathback> files, dirs, links;
    string accessfile;
    struct plfs_pathback pb;

    // first go through and find the set of physical files and dirs
    // that need to be operated on
    // if it's a PLFS file, then maybe we just operate on
    // the access file, or maybe on all subentries
    // if it's a directory, then we operate on all backend copies
    // else just operate on whatever it is (ENOENT, symlink)
    mode_t mode = 0;
    ret = is_container_file(logical,&mode);
    bool is_container = false; // differentiate btwn logical dir and container
    if (S_ISREG(mode)) { // it's a PLFS file
        if (op.onlyAccessFile()) {
            pb.bpath = Container::getAccessFilePath(path);
            pb.back = expansion_info.backend;
            files.push_back(pb);
            ret = 0;    // ret was one from is_container_file
        } else {
            // everything
            is_container = true;
            accessfile = Container::getAccessFilePath(path);
            ret = plfs_collect_from_containers(logical,files,dirs,links);
        }
    } else if (S_ISDIR(mode)) { // need to iterate across dirs
        ret = find_all_expansions(logical,dirs);
    } else {
        // ENOENT, a symlink, somehow a flat file in here
        pb.bpath = path;
        pb.back = expansion_info.backend;
        files.push_back(pb);  // we might want to reset ret to 0 here
    }
    // now apply the operation to each operand so long as ret==0.  dirs must be
    // done in reverse order and files must be done first.  This is necessary
    // for when op is unlink since children must be unlinked first.  for the
    // other ops, order doesn't matter.
    vector<plfs_pathback>::reverse_iterator ritr;
    for(ritr = files.rbegin(); ritr != files.rend() && ret == 0; ++ritr) {
        // In container mode, we want to special treat accessfile deletion,
        // because once accessfile deleted, the top directory will no longer
        // be viewed as a container. Defer accessfile deletion until last moment
        // so that if anything fails in the middle, the container information
        // remains.
        if (is_container && accessfile == ritr->bpath) {
            mlog(INT_DCOMMON, "%s skipping accessfile %s",
                              __FUNCTION__, ritr->bpath.c_str());
            continue;
        }
        mlog(INT_DCOMMON, "%s on %s",__FUNCTION__,ritr->bpath.c_str());
        ret = op.op(ritr->bpath.c_str(),DT_REG,ritr->back->store); 
    }
    for(ritr = links.rbegin(); ritr != links.rend() && ret == 0; ++ritr) {
        op.op(ritr->bpath.c_str(),DT_LNK,ritr->back->store);
    }
    for(ritr = dirs.rbegin(); ritr != dirs.rend() && ret == 0; ++ritr) {
        if (is_container && ritr->bpath == path) {
            mlog(INT_DCOMMON, "%s skipping canonical top directory%s",
                              __FUNCTION__, path.c_str());
            continue;
        }
        ret = op.op(ritr->bpath.c_str(),is_container?DT_CONTAINER:DT_DIR,
                    ritr->back->store);
    }
    if (is_container) {
        mlog(INT_DCOMMON, "%s processing access file and canonical top dir",
                          __FUNCTION__);
        ret = op.op(accessfile.c_str(),DT_REG,expansion_info.backend->store);
        if (ret == 0)
            ret = op.op(path.c_str(),DT_CONTAINER,
                        expansion_info.backend->store);
    }
    mlog(INT_DAPI, "%s: ret %d", __FUNCTION__,ret);
    PLFS_EXIT(ret);
}

int
ContainerFileSystem::open(Plfs_fd **pfd, const char *logical, int flags,
                          pid_t pid, mode_t mode, Plfs_open_opt *open_opt)
{
    int ret;
    bool newly_created = false;
    // possible that we just reuse the current one (XXXCDC: WHEN?)
    // or we need to make a new open
    if (*pfd == NULL) {
        newly_created = true;
        *pfd = new Container_fd();
    }
    ret = (*pfd)->open(logical, flags, pid, mode, open_opt);
    if (ret != 0 && newly_created) {
        delete (*pfd);
        *pfd = NULL;
    }
    return ret;
}

int
ContainerFileSystem::create(const char *logical, mode_t mode,
                            int flags, pid_t pid)
{
    PLFS_ENTER;
    flags = O_WRONLY|O_CREAT|O_TRUNC;
    // for some reason, the ad_plfs_open that calls this passes a mode
    // that fails the S_ISREG check... change to just check for fifo
    //if (!S_ISREG(mode)) {  // e.g. mkfifo might need to be handled differently
    if (S_ISFIFO(mode)) {
        mlog(PLFS_DRARE, "%s on non-regular file %s?",__FUNCTION__, logical);
        PLFS_EXIT(-ENOSYS);
    }
    // ok.  For instances in which we ALWAYS want shadow containers such
    // as we have a canonical location which is remote and slow and we want
    // ALWAYS to store subdirs in faster shadows, then we want to create
    // the subdir's lazily.  This means that the subdir will not be created
    // now and later when procs try to write to the file, they will discover
    // that the subdir doesn't exist and they'll set up the shadow and the
    // metalink at that time
    bool lazy_subdir = false;
    if (expansion_info.mnt_pt->shadowspec != NULL) {
        // ok, user has explicitly set a set of shadow_backends
        // this suggests that the user wants the subdir somewhere else
        // beside the canonical location.  Let's double check though.
        ContainerPaths paths;
        ret = Container::findContainerPaths(logical,paths);
        if (ret!=0) {
            PLFS_EXIT(ret);
        }
        lazy_subdir = !(paths.shadow==paths.canonical);
        mlog(INT_DCOMMON, "Due to explicit shadow_backends directive, setting "
             "subdir %s to be created %s\n",
             paths.shadow.c_str(),
             (lazy_subdir?"lazily":"eagerly"));
    }
    int attempt = 0;
    ret =  Container::create(path,expansion_info.backend,
                             Util::hostname(),mode,flags,
                             &attempt,pid,expansion_info.mnt_pt->checksum,
                             lazy_subdir);
    PLFS_EXIT(ret);
}

int
ContainerFileSystem::chown(const char *logical, uid_t u, gid_t g)
{
    PLFS_ENTER;
    ChownOp op(u,g);
    op.ignoreErrno(-ENOENT); // see comment in container_utime
    ret = plfs_file_operation(logical,op);
    PLFS_EXIT(ret);
}

int
ContainerFileSystem::chmod(const char *logical, mode_t mode)
{
    PLFS_ENTER;
    ChmodOp op(mode);
    ret = plfs_file_operation(logical,op);
    PLFS_EXIT(ret);
}

int
ContainerFileSystem::getmode(const char *logical, mode_t *mode)
{
    PLFS_ENTER;
    *mode = Container::getmode(path, expansion_info.backend);
    PLFS_EXIT(ret);
}

int
ContainerFileSystem::access(const char *logical, int mask)
{
    // possible they are using container_access to check non-plfs file....
    PLFS_ENTER2(PLFS_PATH_NOTREQUIRED);
    if (expansion_info.expand_error) {
        /*
         * XXXCDC: can this really happen?  and if it does how would you
         * know what backend to use since the expand failed?  doesn't
         * make sense to call access on a logical path....
         */
        mlog(MLOG_CRIT, "container_access on bad file %s", logical);
        /*  ret = Util::Access(logical,mask); */
        ret = -EIO;
    } else {
        // oh look.  someone here is using PLFS for its intended purpose to
        // access an actual PLFS entry.  And look, it's so easy to handle!
        AccessOp op(mask);
        ret = plfs_file_operation(logical,op);
    }
    PLFS_EXIT(ret);
}

int
ContainerFileSystem::rename(const char *logical, const char *to)
{
    PLFS_ENTER;
    string old_canonical = path;
    string old_canonical_backend = get_backend(expansion_info);
    string new_canonical;
    string new_canonical_backend;
    mlog(INT_DAPI, "%s: %s -> %s", __FUNCTION__, logical, to);
    // first check if there is a file already at dst.  If so, remove it
    ExpansionInfo exp_info;
    new_canonical = expandPath(to,&exp_info,EXPAND_CANONICAL,-1,0);
    new_canonical_backend = get_backend(exp_info);
    if (exp_info.Errno) {
        PLFS_EXIT(-ENOENT);    // should never happen; check anyway
    }
    struct plfs_pathback npb;
    npb.bpath = new_canonical;
    npb.back = exp_info.backend;
    if (is_container_file(to, NULL)) {
        ret = container_unlink(to);
        if (ret!=0) {
            PLFS_EXIT(ret);
        }
    }
    // now check whether it is a file of a directory we are renaming
    mode_t mode;
    struct plfs_pathback opb;
    opb.bpath = old_canonical;
    opb.back = expansion_info.backend;
    bool isfile = Container::isContainer(&opb,&mode);
   
    // for dirs and containers, iterate over all backends and
    // do a rename on each backend.  Symlinks do single rename
    // potentially from one backend to another 
    if (S_ISLNK(mode)) {
        ret = Util::CopyFile( old_canonical.c_str(), opb.back->store,
                              new_canonical.c_str(), npb.back->store);
        if (ret == 0){
            ret = container_unlink(logical);
        }
        PLFS_EXIT(ret);
    }

    // Call unlink here because it does a check to determine whether a 
    // a directory is empty or not.  If the directory is not empty this
    // function will not proceed because rename does not work on 
    // a non-empty destination 
    ret = container_unlink(to);
    if (ret == -ENOTEMPTY ) {
        PLFS_EXIT(ret);
    }
    
    // get the list of all possible entries for both src and dest
    vector<plfs_pathback> srcs, dsts;
    vector<plfs_pathback>::iterator itr;
    if ( (ret = find_all_expansions(logical,srcs)) != 0 ) {
        PLFS_EXIT(ret);
    }
    if ( (ret = find_all_expansions(to,dsts)) != 0 ) {
        PLFS_EXIT(ret);
    }
    assert(srcs.size()==dsts.size());
    // now go through and rename all of them (ignore ENOENT)
    for(size_t i = 0; i < srcs.size(); i++) {
        int err;
        struct plfs_backend *curback;

        curback = srcs[i].back;
        /*
         * find_all_expansions should keep backends in sync between
         * srcs[i] and dsts[i], but check anyway...
         */
        assert(curback == dsts[i].back);
        err = curback->store->Rename(srcs[i].bpath.c_str(),
                                     dsts[i].bpath.c_str());
        if (err == -ENOENT) {
            err = 0;    // a file might not be distributed on all
        }
        if (err != 0) {
            ret = err;    // keep trying but save the error
        }
        mlog(INT_DCOMMON, "rename %s to %s: %d",
             srcs[i].bpath.c_str(),dsts[i].bpath.c_str(),err);
    }
    // if it's a file whose canonical location has moved, recover it
    bool moved = (expansion_info.backend != exp_info.backend);
    if (moved && isfile) {
        // ok, old canonical is no longer a valid path bec we renamed it
        // we need to construct the new path to where the canonical contents are
        // to contains the mount point plus the path.  We just need to rip the
        // mount point off to and then append it to the old_canonical_backend
        string mnt_pt = exp_info.mnt_pt->mnt_pt;
        old_canonical = old_canonical_backend + "/" + &to[mnt_pt.length()];
        opb.bpath = old_canonical;
        ret = Container::transferCanonical(&opb,&npb,
                                           old_canonical_backend,
                                           new_canonical_backend,mode);
    }
    PLFS_EXIT(ret);
}

// do this one basically the same as container_symlink
// this one probably can't work actually since you can't hard link a directory
// and plfs containers are physical directories
int
ContainerFileSystem::link(const char *logical, const char *to)
{
    PLFS_ENTER2(PLFS_PATH_NOTREQUIRED);
    *(&ret) = 0;    // suppress warning about unused variable
    mlog(PLFS_DAPI, "Can't make a hard link to a container." );
    PLFS_EXIT(-ENOSYS);
}

// OK.  This is a bit of a pain.  We've seen cases
// where untar opens a file for writing it and while
// it's open, it initiates a utime on it and then
// while the utime is pending, it closes the file
// this means that the utime op might have found
// an open dropping and is about to operate on it
// when it disappears.  So we need to ignore ENOENT.
// a bit ugly.  Probably we need to do the same
// thing with chown
// returns 0 or -err
int
ContainerFileSystem::utime(const char *logical, struct utimbuf *ut)
{
    PLFS_ENTER;
    UtimeOp op(ut);
    op.ignoreErrno(-ENOENT);
    ret = plfs_file_operation(logical,op);
    PLFS_EXIT(ret);
}

// this should only be called if the uid has already been checked
// and is allowed to access this file
// Container_OpenFile can be NULL
// note: we can't do anythhing with sz_only at this level since we
// don't have an open file
// returns 0 or -err
int
ContainerFileSystem::getattr(const char *logical, struct stat *stbuf,
                             int sz_only)
{
    PLFS_ENTER;
    mode_t mode;
    mlog(PLFS_DAPI, "%s on logical %s (%s)", __FUNCTION__, logical,
         path.c_str());
    memset(stbuf, 0, sizeof(*stbuf));  /* XXX: necessary? */

    if (!is_container_file(logical, &mode)) {
        /* note: is_container_file API fails with mode 0 on ENOENT */
        ret = (mode == 0) ? -ENOENT :
            expansion_info.backend->store->Lstat(path.c_str(), stbuf);
    } else {
        ret = Container::getattr(path, expansion_info.backend, stbuf);
        mode = S_IFREG;
    }

    mlog(PLFS_DAPI, "%s(%s) = %d (mode=%d)", __FUNCTION__, logical,
         ret, mode);

    PLFS_EXIT(ret);
}

int
ContainerFileSystem::trunc(const char *logical, off_t offset, int open_file)
{
    /* XXXCDC: container_trunc has multiple callers too */
    return container_trunc(NULL, logical, offset, open_file);
}

// TODO:  We should perhaps try to make this be atomic.
// Currently it is just gonna to try to remove everything
// if it only does a partial job, it will leave something weird
int
ContainerFileSystem::unlink(const char *logical)
{
    PLFS_ENTER;
    UnlinkOp op;  // treats file and dirs appropriately

    string unlink_canonical = path;
    string unlink_canonical_backend = get_backend(expansion_info);
    struct plfs_pathback unpb;
    unpb.bpath = unlink_canonical;
    unpb.back = expansion_info.backend;

    struct stat stbuf;
    if ( (ret = unpb.back->store->Lstat(unlink_canonical.c_str(),&stbuf)) != 0)  {
        PLFS_EXIT(ret);
    }
    mode_t mode = Container::getmode(unlink_canonical, expansion_info.backend);
    // ignore ENOENT since it is possible that the set of files can contain
    // duplicates
    // duplicates are possible bec a backend can be defined in both
    // shadow_backends and backends

    op.ignoreErrno(-ENOENT);
    ret = plfs_file_operation(logical,op);
    // if the directory is not empty, need to restore backends to their 
    // previous state
    if (ret == -ENOTEMPTY) {
        CreateOp cop(mode);
        cop.ignoreErrno(-EEXIST);
        plfs_iterate_backends(logical,cop);
        ContainerFileSystem::chown(logical, stbuf.st_uid, stbuf.st_gid);
    }
    PLFS_EXIT(ret);
}

// this has to iterate over the backends and make it everywhere
// like all directory ops that iterate over backends, ignore weird failures
// due to inconsistent backends.  That shouldn't happen but just in case
// returns 0 or -err
int
ContainerFileSystem::mkdir(const char *logical, mode_t mode)
{
    PLFS_ENTER;
    CreateOp op(mode);
    ret = plfs_iterate_backends(logical,op);
    PLFS_EXIT(ret);
}

// vptr needs to be a pointer to a set<string>
// returns 0 or -err
int
ContainerFileSystem::readdir(const char *logical, set<string> *entries)
{
    PLFS_ENTER;
    ReaddirOp op(NULL,entries,false,false);
    ret = plfs_iterate_backends(logical,op);
    PLFS_EXIT(ret);
}

// returns -err for error, otherwise number of bytes read
int
ContainerFileSystem::readlink(const char *logical, char *buf, size_t bufsize)
{
    PLFS_ENTER;
    memset((void *)buf, 0, bufsize);
    ret = expansion_info.backend->store->Readlink(path.c_str(),buf,bufsize);
    mlog(PLFS_DAPI, "%s: readlink %s: %d", __FUNCTION__, path.c_str(),ret);
    PLFS_EXIT(ret);
}


// this has to iterate over the backends and remove it everywhere
// possible with multiple backends that some are empty and some aren't
// so if we delete some and then later discover that some aren't empty
// we need to restore them all
// need to test this corner case probably
// return 0 or -err
int
ContainerFileSystem::rmdir(const char *logical)
{
    PLFS_ENTER;
    // save mode in case we need to restore
    mode_t mode = Container::getmode(path, expansion_info.backend);
    UnlinkOp op;
    ret = plfs_iterate_backends(logical,op);
    // check if we started deleting non-empty dirs, if so, restore
    if (ret==-ENOTEMPTY) {
        mlog(PLFS_DRARE, "Started removing a non-empty directory %s. "
             "Will restore.", logical);
        CreateOp cop(mode);
        cop.ignoreErrno(-EEXIST);
        plfs_iterate_backends(logical,cop); // don't overwrite ret
    }
    PLFS_EXIT(ret);
}

// this is when the user wants to make a symlink on plfs
// very easy, just write whatever the user wants into a symlink
// at the proper canonical location
int
ContainerFileSystem::symlink(const char *logical, const char *to)
{
    PLFS_ENTER2(PLFS_PATH_NOTREQUIRED);
    ExpansionInfo exp_info;
    string topath = expandPath(to, &exp_info, EXPAND_CANONICAL,-1,0);
    if (exp_info.expand_error) {
        PLFS_EXIT(-ENOENT);
    }
    ret = exp_info.backend->store->Symlink(logical, topath.c_str());
    mlog(PLFS_DAPI, "%s: %s to %s: %d", __FUNCTION__,
         path.c_str(), topath.c_str(),ret);
    PLFS_EXIT(ret);
}

// returns 0 or -err
int
ContainerFileSystem::statvfs(const char *logical, struct statvfs *stbuf)
{
    PLFS_ENTER;
    ret = expansion_info.backend->store->Statvfs(path.c_str(), stbuf);
    PLFS_EXIT(ret);
}
