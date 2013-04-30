
#include "plfs.h"
#include "plfs_private.h"
#include "IOStore.h"
#include "Index.h"
#include "WriteFile.h"
#include "Container.h"
#include "Util.h"
#include "OpenFile.h"
#include "ThreadPool.h"
#include "FileOp.h"
#include "container_internals.h"
#include "ContainerFS.h"
#include "mlog_oss.h"

#include <list>
#include <stdarg.h>
#include <limits>
#include <limits.h>
#include <assert.h>
#include <queue>
#include <vector>
#include <sstream>
#include <stdlib.h>
#include <ctype.h>

using namespace std;

ssize_t plfs_reference_count( Container_OpenFile * );

// this should only be called if the uid has already been checked
// and is allowed to access this file
// Container_OpenFile can be NULL
// returns 0 or -err
int
container_getattr(Container_OpenFile *of, const char *logical,
                  struct stat *stbuf,int sz_only)
{
    // ok, this is hard
    // we have a logical path maybe passed in or a physical path
    // already stashed in the of
    // this backward stuff might be deprecated.  We should check and remove.
    bool backwards = false;
    if ( logical == NULL ) {
        logical = of->getPath();    // this is the physical path
        backwards = true;
    }
    PLFS_ENTER; // this assumes it's operating on a logical path
    if ( backwards ) {
        //XXXCDC: can't happen if physical, since PLFS_ENTER will fail+exit??
        path = of->getPath();   // restore the stashed physical path
        expansion_info.backend = of->getCanBack(); //XXX
    }
    mlog(PLFS_DAPI, "%s on logical %s (%s)", __FUNCTION__, logical,
         path.c_str());
    memset(stbuf,0,sizeof(struct stat));    // zero fill the stat buffer
    mode_t mode = 0;
    if ( ! is_container_file( logical, &mode ) ) {
        // this is how a symlink is stat'd bec it doesn't look like a plfs file
        if ( mode == 0 ) {
            ret = -ENOENT;
        } else {
            mlog(PLFS_DCOMMON, "%s on non plfs file %s", __FUNCTION__,
                 path.c_str());
            ret = expansion_info.backend->store->Lstat(path.c_str(),stbuf);
        }
    } else {    // operating on a plfs file here
        // there's a lazy stat flag, sz_only, which means all the caller
        // cares about is the size of the file.  If the file is currently
        // open (i.e. we have a wf ptr, then the size info is stashed in
        // there.  It might not be fully accurate since it just contains info
        // for the writes of the current proc but it's a good-enough estimate
        // however, if the caller hasn't passed lazy or if the wf isn't
        // available then we need to do a more expensive descent into
        // the container.  This descent is especially expensive for an open
        // file where we can't just used the cached meta info but have to
        // actually fully populate an index structure and query it
        WriteFile *wf=(of && of->getWritefile() ? of->getWritefile() :NULL);
        bool descent_needed = ( !sz_only || !wf || (of && of->isReopen()) );
        if (descent_needed) {  // do we need to descend and do the full?
            ret = Container::getattr( path, expansion_info.backend, stbuf );
            mlog(PLFS_DCOMMON, "descent_needed, "
                 "Container::getattr ret :%d.\n", ret);
        }
        if (ret == 0 && wf) {
            off_t  last_offset;
            size_t total_bytes;
            wf->getMeta( &last_offset, &total_bytes );
            mlog(PLFS_DCOMMON, "Got meta from openfile: %lu last offset, "
                 "%ld total bytes", (unsigned long)last_offset,
                 (unsigned long)total_bytes);
            if ( last_offset > stbuf->st_size ) {
                stbuf->st_size = last_offset;
            }
            if ( ! descent_needed ) {
                // this is set on the descent so don't do it again if descended
                stbuf->st_blocks = Container::bytesToBlocks(total_bytes);
            }
        }
    }
    if ( ret != 0 ) {
        mlog(PLFS_DRARE, "logical %s,stashed %s,physical %s: %s",
             logical,of?of->getPath():"NULL",path.c_str(),
             strerror(-ret));
    }
    mss::mlog_oss oss(PLFS_DAPI);
    oss << __FUNCTION__ << " of " << path << "("
        << (of == NULL ? "closed" : "open")
        << ") size is " << stbuf->st_size;
    oss.commit();
    PLFS_EXIT(ret);
}

size_t container_gethostdir_id(char *hostname)
{
    return Container::getHostDirId(hostname);
}

/*
 * Nothing was calling this function, so I deleted it.
 *
int
container_dump_index_size()
{
    ContainerEntry e;
    cout << "An index entry is size " << sizeof(e) << endl;
    return (int)sizeof(e);
}
 */

// returns 0 or -err
int
container_dump_index( FILE *fp, const char *logical, int compress, 
        int uniform_restart, pid_t uniform_restart_rank )
{
    PLFS_ENTER;
    Index index(path, expansion_info.backend);
    ret = Container::populateIndex(
            path,expansion_info.backend,&index,true,uniform_restart,
            uniform_restart_rank);
    if ( ret == 0 ) {
        if (compress) {
            index.compress();
        }
        ostringstream oss;
        oss << index;
        fprintf(fp,"%s",oss.str().c_str());
    }
    PLFS_EXIT(ret);
}

int
is_container_file( const char *logical, mode_t *mode )
{
    PLFS_ENTER;
    struct plfs_pathback pb;
    pb.bpath = path;
    pb.back = expansion_info.backend;
    ret = Container::isContainer(&pb,mode);
    PLFS_EXIT(ret);
}

// this is a bit of a crazy function.  Basically, it's for the case where
// someone changed the set of backends for an existing mount point.  They
// shouldn't ever do this, so hopefully this code is never used!
// But if they
// do, what will happen is that they will see their file on a readdir() but on
// a stat() they'll either get ENOENT because there is nothing at the new
// canonical location, or they'll see the shadow container which looks like a
// directory to them.
// So this function makes it so that a plfs file that had a
// different previous canonical location is now recovered to the new canonical
// location.  hopefully it always works but it won't currently work across
// different file systems because it uses rename()
// returns 0 or -err (-EEXIST means it didn't need to be recovered)
// TODO: this should be made specific to container.  any general code
// should be moved out
int
container_recover(const char *logical)
{
    PLFS_ENTER;
    string canonical, former_backend, canonical_backend;
    bool found, isdir, isfile;
    mode_t canonical_mode = 0, former_mode = 0;
    struct plfs_pathback canonical_pb, former;
    // then check whether it's is already at the correct canonical location
    // however, if we find a directory at the correct canonical location
    // we still need to keep looking bec it might be a shadow container
    canonical = path;
    canonical_backend = get_backend(expansion_info);
    mlog(PLFS_DAPI, "%s Canonical location should be %s", __FUNCTION__,
         canonical.c_str());
    canonical_pb.bpath = path;
    canonical_pb.back = expansion_info.backend;
    isfile = (int) Container::isContainer(&canonical_pb,&canonical_mode);
    if (isfile) {
        mlog(PLFS_DCOMMON, "%s %s is already in canonical location",
             __FUNCTION__, canonical.c_str());
        PLFS_EXIT(-EEXIST);
    }
    mlog(PLFS_DCOMMON, "%s %s may not be in canonical location",
         __FUNCTION__,logical);
    // ok, it's not at the canonical location
    // check all the other backends to see if they have it
    // also check canonical bec it's possible it's a dir that only exists there
    isdir = false;  // possible we find it and it's a directory
    isfile = false; // possible we find it and it's a container
    found = false;  // possible it doesn't exist (ENOENT)
    vector<plfs_pathback> exps;
    if ( (ret = find_all_expansions(logical,exps)) != 0 ) {
        PLFS_EXIT(ret);
    }
    for(size_t i=0; i<exps.size(); i++) {
        plfs_pathback possible = exps[i];
        ret  = (int) Container::isContainer(&possible,&former_mode);
        if (ret) {
            isfile = found = true;
            former = possible;
            // we know the backend is at offset i in backends
            // we know this is in the same mount point as canonical
            // that mount point is still stashed in expansion_info
            former_backend = get_backend(expansion_info,i);
            break;  // no need to keep looking
        } else if (S_ISDIR(former_mode)) {
            isdir = found = true;
        }
        mlog(PLFS_DCOMMON, "%s query %s: %s", __FUNCTION__,
             possible.bpath.c_str(),
             (isfile?"file":isdir?"dir":"ENOENT"));
    }
    if (!found) {
        PLFS_EXIT(-ENOENT);
    }
    // if we make it here, we found a file or a dir at the wrong location
    // dirs are easy
    if (isdir && !isfile) {
        PLFS_EXIT(recover_directory(logical,false));
    }
    // if we make it here, it's a file
    // first recover the parent directory, then ensure a container directory
    // if performance is ever slow here, we probably don't need to recover
    // the parent directory here
    if ((ret = recover_directory(logical,true)) != 0) {
        PLFS_EXIT(ret);
    }
    ret = mkdir_dash_p(canonical,false,canonical_pb.back->store);
    if (ret != 0 && ret != EEXIST) {
        PLFS_EXIT(ret);    // some bad error
    }
    ret = Container::transferCanonical(&former,&canonical_pb,
                                       former_backend,canonical_backend,
                                       former_mode);
    if ( ret != 0 ) {
        printf("Unable to recover %s.\nYou may be able to recover the file"
               " by manually moving contents of %s to %s\n",
               logical,
               former.bpath.c_str(),
               canonical_pb.bpath.c_str());
    }
    PLFS_EXIT(ret);
}

// Function that reads in the hostdirs and sets the bitmap
// this function still works even with metalink stuff
// probably though we should make an opaque function in
// Container.cpp that encapsulates this....
// returns -err if the opendir fails
// returns -EISDIR if it's actually a directory and not a file
// returns a positive number otherwise as even an empty container
// will have at least one hostdir
// hmmm.  this function does a readdir.  be nice to move this into
// library and use new readdirop class

int
container_num_host_dirs(int *hostdir_count,char *target, void *vback, char *bm)
{
    // Directory reading variables
    IOStore *store = ((plfs_backend *)vback)->store;
    IOSDirHandle *dirp;
    struct dirent entstore, *dirent;
    int isfile = 0, ret = 0, rv;
    *hostdir_count = 0;
    // Open the directory and check value
    if ((dirp = store->Opendir(target,ret)) == NULL) {
        mlog(PLFS_DRARE, "Num hostdir opendir error on %s",target);
        // XXX why?
        *hostdir_count = ret;
        return *hostdir_count;
    }
    // Start reading the directory
    while (dirp->Readdir_r(&entstore, &dirent) == 0 && dirent != NULL) {
        // Look for entries that beging with hostdir
        if(strncmp(HOSTDIRPREFIX,dirent->d_name,strlen(HOSTDIRPREFIX))==0) {
            char *substr;
            substr=strtok(dirent->d_name,".");
            substr=strtok(NULL,".");
            int index = atoi(substr);
            if (index>=MAX_HOSTDIRS) {
                fprintf(stderr,"Bad behavior in PLFS.  Too many subdirs.\n");
                *hostdir_count = -ENOSYS;
                return *hostdir_count;
            }
            mlog(PLFS_DCOMMON,"Added a hostdir for %d", index);
            (*hostdir_count)++;
            //adplfs_setBit(index,bitmap);
            long whichByte = index / 8;
            long whichBit = index % 8;
            char temp = bm[whichByte];
            bm[whichByte] = (char)(temp | (0x80 >> whichBit));
            //adplfs_setBit(index,bitmap);
        } else if (strncmp(ACCESSFILE,dirent->d_name,strlen(ACCESSFILE))==0) {
            isfile = 1;
        }
    }
    // Close the dir error out if we have a problem
    if ((rv = store->Closedir(dirp)) < 0) {
        mlog(PLFS_DRARE, "Num hostdir closedir error on %s",target);
        *hostdir_count = rv;
        return(rv);
    }
    mlog(PLFS_DCOMMON, "%s of %s isfile %d hostdirs %d",
               __FUNCTION__,target,isfile,*hostdir_count);
    if (!isfile) {
        *hostdir_count = -EISDIR;
    }
    return *hostdir_count;
}

/**
 * container_hostdir_rddir: function called from MPI open when #hostdirs>#procs.
 * this function is used under MPI (called only by adplfs_read_and_merge).
 *
 * @param index_stream buffer to place result in
 * @param targets bpaths of hostdirs in canonical, sep'd with '|'
 * @param rank the MPI rank of caller
 * @param top_level bpath to canonical container dir
 * @param pmount void pointer to PlfsMount of logical file
 * @param pback void pointer to plfs_backend of canonical container
 * @return # output bytes in index_stream or -err
 */
int
container_hostdir_rddir(void **index_stream,char *targets,int rank,
                   char *top_level, void *pmount, void *pback)
{
    PlfsMount *mnt = (PlfsMount *)pmount;
    struct plfs_backend *canback = (struct plfs_backend *)pback;
    size_t stream_sz;
    string path;
    vector<string> directories;
    vector<IndexFileInfo> index_droppings;
    mlog(INT_DCOMMON, "Rank |%d| targets %s",rank,targets);
    Util::tokenize(targets,"|",directories);
    // Path is extremely important when converting to stream
    Index global(top_level,canback);
    unsigned count=0;
    while(count<directories.size()) {
        struct plfs_backend *idxback;
        path=directories[count];   /* a single hostdir (could be metalink) */
        /*
         * this call will resolve the metalink (if there is one) and
         * then read the list of indices from the current subdir into
         * the IndexFileInfo index_droppings.
         */
        int ret = Container::indices_from_subdir(path, mnt, canback,
                                                 &idxback, index_droppings);
        if (ret!=0) {
            return ret;
        }
        /* discard un-needed special first 'path holder' entry of droppings */
        index_droppings.erase(index_droppings.begin());
        Index tmp(top_level,canback);
        /*
         * now we use parAggregateIndices() to read each index file
         * listed for this subdir in the index_droppings into a single
         * Index (returned in tmp).   we then merge this into our
         * "global" result, which is the index records for all subdirs
         * assigned for this rank.   parAggregateIndices uses a thread
         * pool to read the index data in parallel.
         */
        tmp=Container::parAggregateIndices(index_droppings,0,1,
                                           path,idxback);
        global.merge(&tmp);
        count++;
    }
    /*
     * done.  convert return value back to stream.   each rank will
     * eventually collect all "global" values from the other ranks in
     * function adplfs_read_and_merge() and merge them all into
     * one single global index for the file.
     */
    global.global_to_stream(index_stream,&stream_sz);
    return (int)stream_sz;
}

/**
 * container_hostdir_zero_rddir: called from MPI open when #procs>#subdirs,
 * so there are a set of procs assigned to one subdir.  the comm has
 * been split so there is a rank 0 for each subdir.  each rank 0 calls
 * this to resolve the metalink and get the list of index files in
 * this subdir.  note that the first entry of the returned list is
 * special and contains the 'path holder' bpath of subdir (with all
 * metalinks resolved -- see indices_from_subdir).
 *
 * @param entries ptr to resulting list of IndexFileInfo put here
 * @param path the bpath of hostdir in canonical container
 * @param rank top-level rank (not the split one)
 * @param pmount logical PLFS mount point where file being open resides
 * @param pback the the canonical backend
 * @return size of hostdir stream entries or -err
 */
int
container_hostdir_zero_rddir(void **entries,const char *path,int rank,
                        void *pmount, void *pback)
{
    PlfsMount *mnt = (PlfsMount *)pmount;
    struct plfs_backend *canback = (struct plfs_backend *)pback;
    vector<IndexFileInfo> index_droppings;
    struct plfs_backend *idxback;
    int size;
    IndexFileInfo converter;
    int ret = Container::indices_from_subdir(path, mnt, canback, &idxback,
                                             index_droppings);
    if (ret!=0) {
        return ret;
    }
    mlog(INT_DCOMMON, "Found [%lu] index droppings in %s",
         (unsigned long)index_droppings.size(),path);
    *entries=converter.listToStream(index_droppings,&size);
    return size;
}

/**
 * container_parindex_read: called from MPI open's split and merge code path
 * to read a set of index files in a hostdir on a single backend.
 *
 * @param rank our rank in the split MPI communicator
 * @param ranks_per_comm number of ranks in the comm
 * @param index_files stream of IndexFileInfo recs from indices_from_subdir()
 * @param index_stream resulting combined index stream goes here (output)
 * @param top_level bpath to canonical container
 * @return size of index or error
 */
int
container_parindex_read(int rank,int ranks_per_comm,void *index_files,
                   void **index_stream,char *top_level)
{
    size_t index_stream_sz;
    vector<IndexFileInfo> cvt_list;
    IndexFileInfo converter;
    string phys,bpath,index_path;
    struct plfs_backend *backend;
    int rv;
    cvt_list = converter.streamToList(index_files);
    
    /*
     * note that the first entry in cvt_list has the physical path of
     * the hostdir (post Metalink processing) stored in the hostname
     * field (see indices_from_subdir).   we need to extract that and
     * map it back to the backend.
     */
    phys=cvt_list[0].hostname;
    rv = plfs_phys_backlookup(phys.c_str(), NULL, &backend, &bpath);
    if (rv != 0) {
        /* this shouldn't ever happen */
        mlog(INT_CRIT, "container_parindex_read: %s: backlookup failed?",
             phys.c_str());
        return(rv);
    }
    mlog(INT_DCOMMON, "Hostdir path pushed on the list %s (bpath=%s)",
         phys.c_str(), bpath.c_str());
    mlog(INT_DCOMMON, "Path: %s used for Index file in parindex read",
         top_level);

    /*
     * allocate a temporary index object to store the data in.  read it
     * with paraggregateIndices(), then serialize it into a buffer using
     * global_to_stream.
     *
     * XXX: the path isn't really needed anymore (used to be when
     * global_to_stream tried to optimize the chunk path strings by
     * stripping the common parts, but we don't do that anymore).
     */
    Index index(top_level, NULL);
    cvt_list.erase(cvt_list.begin());  /* discard first entry on list */
    //Everything seems fine at this point
    mlog(INT_DCOMMON, "Rank |%d| List Size|%lu|",rank,
         (unsigned long)cvt_list.size());
    index=Container::parAggregateIndices(cvt_list,rank,ranks_per_comm,
                                         bpath,backend);
    mlog(INT_DCOMMON, "Ranks |%d| About to convert global to stream",rank);
    // Don't forget to trick global to stream
    index_path=top_level;        /* XXX: not needed anymore */
    index.setPath(index_path);   /* XXX: not needed anymore */
    // Index should be populated now
    index.global_to_stream(index_stream,&index_stream_sz);
    return (int)index_stream_sz;
}

// TODO: change name to container_*
int
container_merge_indexes(Plfs_fd **fd_in, char *index_streams,
                   int *index_sizes, int procs)
{
    Container_OpenFile **pfd = (Container_OpenFile **)fd_in;
    int count;
    Index *root_index;
    mlog(INT_DAPI, "Entering container_merge_indexes");
    // Root has no real Index set it to the writefile index
    mlog(INT_DCOMMON, "Setting writefile index to pfd index");
    (*pfd)->setIndex((*pfd)->getWritefile()->getIndex());
    mlog(INT_DCOMMON, "Getting the index from the pfd");
    root_index=(*pfd)->getIndex();
    for(count=1; count<procs; count++) {
        char *index_stream;
        // Skip to the next index
        index_streams+=(index_sizes[count-1]);
        index_stream=index_streams;
        // Turn the stream into an index
        mlog(INT_DCOMMON, "Merging the stream into one Index");
        // Merge the index
        root_index->global_from_stream(index_stream);
        mlog(INT_DCOMMON, "Merge success");
        // Free up the memory for the index stream
        mlog(INT_DCOMMON, "Index stream free success");
    }
    mlog(INT_DAPI, "%s:Done merging indexes",__FUNCTION__);
    return 0;
}

/*
 * this one takes a set of "procs" index streams in index_streams in
 * memory and merges them into one index stream, result saved in
 * "index_stream" pointer...   this is all in memory, no threads
 * used.
 * path
 * index_streams: byte array, variable length, procs records
 * index_sizes: record length, procs entries
 * index_stream: output goes here
 */
int
container_parindexread_merge(const char *path,char *index_streams,
                        int *index_sizes, int procs, void **index_stream)
{
    int count;
    size_t size;
    Index merger(path, NULL);  /* temporary obj use for collection */
    // Merge all of the indices that were passed in
    for(count=0; count<procs; count++) {
        char *istream;
        if(count>0) {
            int index_inc=index_sizes[count-1];
            mlog(INT_DCOMMON, "Incrementing the index by %d",index_inc);
            index_streams+=index_inc;
        }
        Index *tmp = new Index(path, NULL);
        istream=index_streams;
        tmp->global_from_stream(istream);
        merger.merge(tmp);
    }
    // Convert temporary merger Index object into a stream and return that
    merger.global_to_stream(index_stream,&size);
    mlog(INT_DCOMMON, "Inside parindexread merge stream size %lu",
         (unsigned long)size);
    return (int)size;
}

// Can't directly access the FD struct in ADIO
// TODO: change name to container_*
int
container_index_stream(Plfs_fd **fd_in, char **buffer)
{
    Container_OpenFile **pfd = (Container_OpenFile **)fd_in;
    size_t length;
    int ret;
    if ( (*pfd)->getIndex() !=  NULL ) {
        mlog(INT_DCOMMON, "Getting index stream from a reader");
        ret = (*pfd)->getIndex()->global_to_stream((void **)buffer,&length);
    } else if( (*pfd)->getWritefile()->getIndex()!=NULL) {
        mlog(INT_DCOMMON, "The write file has the index");
        ret = (*pfd)->getWritefile()->getIndex()->global_to_stream(
                  (void **)buffer,&length);
    } else {
        mlog(INT_DRARE, "Error in container_index_stream");
        return -1;
    }
    mlog(INT_DAPI,"In container_index_stream global to stream has size %lu ret=%d",
         (unsigned long)length, ret);
    return length;
}

// I don't like this function right now
// why does it have hard-coded numbers in it like programName[64] ?
// TODO: should this function be in this file?
// TODO: describe this function.  what is it?  what does it do?
// XXXCDC: need to pass srcback/dstback to worker program
int
initiate_async_transfer(const char *src, const char *srcprefix,
                        const char *dest_dir, const char *dstprefix,
                        const char *syncer_IP)
{
    int rc;
    char space[2];
    char programName[64];
    char *command;
    char commandList[2048] ;
    mlog(INT_DAPI, "Enter %s  \n", __FUNCTION__);
    memset(&commandList, '\0', 2048);
    memset(&programName, '\0', 64);
    memset(&space, ' ', 2);
    strcpy(programName, "SYNcer  ");
    mlog(INT_DCOMMON, "systemDataMove  0001\n");
//    Never read, as below
//    command  = strcat(commandList, "ssh ");
    command  = strcat(commandList, syncer_IP);
    mlog(INT_DCOMMON, "0B command=%s\n", command);
//    These values are never read, why do the work?
//    command  = strncat(commandList, space, 1);
//    command  = strcat(commandList, programName);
//    command  = strncat(commandList, space, 1);
//    command  = strcat(commandList, src);
//    command  = strncat(commandList, space, 1);
//    command  = strcat(commandList, dest_dir);
//    command  = strncat(commandList, space, 1);
    double start_time,end_time;
    start_time=plfs_wtime();
    rc = system(commandList);
    end_time=plfs_wtime();
    mlog(INT_DCOMMON, "commandList=%s took %.2ld secs, rc: %d.\n", commandList,
         (unsigned long)(end_time-start_time), rc);
    fflush(stdout);
    return rc;
}

// TODO: should this function be in this file?
// TODO: describe this function.  what is it?  what does it do?
int
plfs_find_my_droppings(const string& physical, IOStore *store,
                       pid_t pid, set<string> &drops)
{
    ReaddirOp rop(NULL,&drops,true,false);
    rop.filter(INDEXPREFIX);
    rop.filter(DATAPREFIX);
    int ret = rop.op(physical.c_str(),DT_DIR,store);
    if (ret!=0) {
        PLFS_EXIT(ret);
    }
    // go through and delete all that don't belong to pid
    // use while not for since erase invalidates the iterator
    set<string>::iterator itr = drops.begin();
    while(itr!=drops.end()) {
        set<string>::iterator prev = itr++;
        int dropping_pid = Container::getDroppingPid(*prev);
        if (dropping_pid != getpid() && dropping_pid != pid) {
            drops.erase(prev);
        }
    }
    PLFS_EXIT(0);
}

/* XXXCDC: container_protect is an MPI-only function */
// iterate through container.  Find all pieces owned by this pid that are in
// shadowed subdirs.  Currently do this is a non-transaction unsafe method
// that assumes no failure in the middle.
// 1) blow away metalink in canonical
// 2) create a subdir in canonical
// 3) call SYNCER to move each piece owned by this pid in this subdir
int
container_protect(const char *logical, pid_t pid)
{
    PLFS_ENTER;
    // first make sure that syncer_ip is defined
    // otherwise this doesn't work
    string *syncer_ip = expansion_info.mnt_pt->syncer_ip;
    if (!syncer_ip) {
        mlog(INT_DCOMMON, "Cant use %s with syncer_ip defined in plfsrc\n",
             __FUNCTION__);
        PLFS_EXIT(-ENOSYS);
    }
    // find path to shadowed subdir and make a temporary hostdir
    // in canonical
    ContainerPaths paths;
    ret = Container::findContainerPaths(logical,paths);
    if (ret != 0) {
        PLFS_EXIT(ret);
    }
    string src = paths.shadow_hostdir;
    string dst = Container::getHostDirPath(paths.canonical,Util::hostname(),
                                           TMP_SUBDIR);
    ret = paths.canonicalback->store->Mkdir(dst.c_str(), CONTAINER_MODE);
    if (ret == -EEXIST || ret == -EISDIR ) {
        ret = 0;
    }
    if (ret != 0) {
        PLFS_EXIT(ret);
    }
    mlog(INT_DCOMMON, "Need to protect contents of %s into %s\n",
         src.c_str(),dst.c_str());
    // read the shadowed subdir and find all droppings
    set<string> droppings;
    ret = plfs_find_my_droppings(src,paths.shadowback->store,pid,droppings);
    if (ret != 0) {
        PLFS_EXIT(ret);
    }
    // for each dropping owned by this pid, initiate a replication to canonical
    set<string>::iterator itr;
    for (itr=droppings.begin(); itr!=droppings.end(); itr++) {
        mlog(INT_DCOMMON, "SYNCER %s cp %s %s\n", syncer_ip->c_str(),
             itr->c_str(), dst.c_str());
        initiate_async_transfer(itr->c_str(), paths.shadowback->prefix,
                                dst.c_str(), paths.canonicalback->prefix,
                                syncer_ip->c_str());
    }
    PLFS_EXIT(ret);
}


// void *'s should be vector<string>
// TODO: should this be in this file?
// TODO: should it be renamed to container_locate?
int
container_locate(const char *logical, void *files_ptr,
            void *dirs_ptr, void *metalinks_ptr)
{
    PLFS_ENTER;
    // first, are we locating a PLFS file or a directory or a symlink?
    mode_t mode = 0;
    ret = is_container_file(logical,&mode);
    // do container_locate on a plfs_file
    if (S_ISREG(mode)) { // it's a PLFS file
        vector<plfs_pathback> *files = (vector<plfs_pathback> *)files_ptr;
        vector<string> filters;
        ret = Container::collectContents(path, expansion_info.backend,
                                         *files,
                                         (vector<plfs_pathback>*)dirs_ptr,
                                         (vector<string>*)metalinks_ptr,
                                         filters,true);
        // do container_locate on a plfs directory
    } else if (S_ISDIR(mode)) {
        if (!dirs_ptr) {
            mlog(INT_ERR, "Asked to %s on %s which is a directory but not "
                 "given a vector<string> to store directory paths into...\n",
                 __FUNCTION__,logical);
            ret = -EINVAL;
        } else {
            vector<plfs_pathback> *dirs = (vector<plfs_pathback> *)dirs_ptr;
            ret = find_all_expansions(logical,*dirs);
        }
        // do container_locate on a symlink
    } else if (S_ISLNK(mode)) {
        if (!metalinks_ptr) {
            mlog(INT_ERR, "Asked to %s on %s which is a symlink but not "
                 "given a vector<string> to store link paths into...\n",
                 __FUNCTION__,logical);
            ret = -EINVAL;
        } else {
            ((vector<string> *)metalinks_ptr)->push_back(path);
            ret = 0;
        }
        // something strange here....
    } else {
        // Weird.  What else could it be?
        ret = -ENOENT;
    }
    //*target = path;
    PLFS_EXIT(ret);
}

// this can fail due to silly rename
// imagine an N-1 normal file on PanFS that someone is reading and
// someone else is unlink'ing.  The unlink will see a reference count
// and will rename it to .panfs.blah.  The read continues and when the
// read releases the reference count on .panfs.blah drops to zero and
// .panfs.blah is unlinked
// but in our containers, here's what happens:  a chunk or an index is
// open by someone and is unlinked by someone else, the silly rename
// does the same thing and now the container has a .panfs.blah file in
// it.  Then when we try to remove the directory, we get a ENOTEMPTY.
// truncate only removes the droppings but leaves the directory structure
//
// we also use this function to implement truncate on a container
// in that case, we remove all droppings but preserve the container
// an empty container = empty file
//
// this code should really be moved to container
//
// be nice to use the new FileOp class for this.  Bit tricky though maybe.
// by the way, this should only be called now with truncate_only==true
// we changed the unlink functionality to use the new FileOp stuff
//
// the TruncateOp internally does unlinks
// TODO: rename to container_* ?
int
truncateFileToZero(const string &physical_canonical, struct plfs_backend *back,
                   const char *logical,bool open_file)
{
    int ret;
    TruncateOp op(open_file);
    // ignore ENOENT since it is possible that the set of files can contain
    // duplicates.
    // duplicates are possible bec a backend can be defined in both
    // shadow_backends and backends
    op.ignoreErrno(-ENOENT);
    op.ignore(ACCESSFILE);
    op.ignore(OPENPREFIX);
    op.ignore(VERSIONPREFIX);

    ret = plfs_file_operation(logical,op);
    if (ret == 0 && open_file == 1){
        //if we successfully truncated the file to zero
        //and the file is open, we also need to truncate
        //the metadata droppings
        ret = Container::truncateMeta(physical_canonical, 0, back);
    }
    return ret;
}

int
container_file_version(const char *logical, const char **version)
{
    PLFS_ENTER;
    struct plfs_pathback pb;
    (void)ret; // suppress compiler warning
    mode_t mode;
    if (!is_container_file(logical, &mode)) {
        return -ENOENT;
    }
    pb.bpath = path;
    pb.back = expansion_info.backend;
    *version = Container::version(&pb);
    return (*version ? 0 : -ENOENT);
}

// the Container_OpenFile can be NULL (e.g. if file is not open by us)
// be nice to use new FileOp class for this somehow
// returns 0 or -err
int
container_trunc(Container_OpenFile *of, const char *logical, off_t offset,
                int open_file)
{
    PLFS_ENTER;
    mode_t mode = 0;
    struct stat stbuf;
    stbuf.st_size = 0;
    if ( !of && ! is_container_file( logical, &mode ) ) {
        // this is weird, we expect only to operate on containers
        if ( mode == 0 ) {
            ret = -ENOENT;
        } else {
            ret = expansion_info.backend->store->Truncate(path.c_str(),offset);
        }
        PLFS_EXIT(ret);
    }
    mlog(PLFS_DCOMMON, "%s:%d ret is %d", __FUNCTION__, __LINE__, ret);
    // once we're here, we know it's a PLFS file
    if ( offset == 0 ) {
        // first check to make sure we are allowed to truncate this
        // all the droppings are global so we can truncate them but
        // the access file has the correct permissions
        string access = Container::getAccessFilePath(path);
        ret = expansion_info.backend->store->Truncate(access.c_str(),0);
        mlog(PLFS_DCOMMON, "Tested truncate of %s: %d",access.c_str(),ret);
        if ( ret == 0 ) {
            // this is easy, just remove/trunc all droppings
            ret = truncateFileToZero(path, expansion_info.backend,
                                     logical,(bool)open_file);
        }
    } else {
        // either at existing end, before it, or after it
        bool sz_only = false; // sz_only isn't accurate in this case
        // it should be but the problem is that
        // FUSE opens the file and so we just query
        // the open file handle and it says 0
        ret = container_getattr( of, logical, &stbuf, sz_only );
        mlog(PLFS_DCOMMON, "%s:%d ret is %d", __FUNCTION__, __LINE__, ret);
        if ( ret == 0 ) {
            if ( stbuf.st_size == offset ) {
                ret = 0; // nothing to do
            } else if ( stbuf.st_size > offset ) {
                ret = Container::Truncate(path, offset, // make smaller
                                          expansion_info.backend);
                mlog(PLFS_DCOMMON, "%s:%d ret is %d", __FUNCTION__,
                     __LINE__, ret);
            } else if (stbuf.st_size < offset) {

                /* extending file -- treat as a zero byte write @req offset */
                Container_OpenFile *myopenfd;
                pid_t pid;
                WriteFile *wf;

                mlog(PLFS_DCOMMON, "%s:%d ret is %d", __FUNCTION__,
                     __LINE__, ret);

                myopenfd = of;
                pid = (of) ? of->getPid() : 0; /* from old extendFile */
                
                ret = container_open(&myopenfd, logical, O_WRONLY, pid,
                                     mode, NULL);
                
                if (ret != 0) {

                    mlog(PLFS_INFO,
                         "%s: unexpected container_open(%s) error (%d)",
                         __FUNCTION__, logical, ret);
                    
                } else {
                    uid_t uid = 0;  /* just needed for stats */
                    wf = myopenfd->getWritefile(); /* can't fail */
                    ret = wf->extend(offset);      /* zero byte write */
                    /* ignore close ret, can't do much with it here */
                    (void)container_close(myopenfd, pid, uid, O_WRONLY, NULL);
                }
            }
        }
    }
    mlog(PLFS_DCOMMON, "%s:%d ret is %d", __FUNCTION__, __LINE__, ret);
    // if we actually modified the container, update any open file handle
    if ( ret == 0 && of && of->getWritefile() ) {
        mlog(PLFS_DCOMMON, "%s:%d ret is %d", __FUNCTION__, __LINE__, ret);
        // in the case that extend file, need not truncateHostIndex
        if (offset <= stbuf.st_size) {
            ret = Container::truncateMeta(path, offset, expansion_info.backend);
            if (ret==0) {
                ret = of->getWritefile()->truncate( offset );
            }
        }
        of->truncate( offset );
        // here's a problem, if the file is open for writing, we've
        // already opened fds in there.  So the droppings are
        // deleted/resized and our open handles are messed up
        // it's just a little scary if this ever happens following
        // a rename because the writefile will attempt to restore
        // them at the old path....
        if ( ret == 0 && of && of->getWritefile() ) {
            mlog(PLFS_DCOMMON, "%s:%d ret is %d", __FUNCTION__, __LINE__, ret);
            bool droppings_were_truncd = (offset==0 && open_file);
            ret = of->getWritefile()->restoreFds(droppings_were_truncd);
            if ( ret != 0 ) {
                mlog(PLFS_DRARE, "%s:%d failed: %s",
                     __FUNCTION__, __LINE__, strerror(-ret));
            }
        } else {
            mlog(PLFS_DRARE, "%s failed: %s", __FUNCTION__, strerror(-ret));
        }
        mlog(PLFS_DCOMMON, "%s:%d ret is %d", __FUNCTION__, __LINE__, ret);
    }
    mlog(PLFS_DCOMMON, "%s %s to %u: %d",__FUNCTION__,path.c_str(),
         (uint)offset,ret);
    if ( ret == 0 ) { // update the timestamp
        ret = Container::Utime( path, expansion_info.backend, NULL );
    }
    PLFS_EXIT(ret);
}

// a helper function to make unlink be atomic
// returns a funny looking string that is hopefully unique and then
// tries to remove that
// TODO: should this be in this function?
// TODO: add comment about who might call this and why
string
getAtomicUnlinkPath(string path)
{
    string atomicpath = path + ".plfs_atomic_unlink.";
    stringstream timestamp;
    timestamp << fixed << Util::getTime();
    vector<string> tokens;
    Util::tokenize(path,"/",tokens);
    atomicpath = "";
    for(size_t i=0 ; i < tokens.size(); i++) {
        atomicpath += "/";
        if ( i == tokens.size() - 1 ) {
            atomicpath += ".";    // hide it
        }
        atomicpath += tokens[i];
    }
    atomicpath += ".plfs_atomic_unlink.";
    atomicpath.append(timestamp.str());
    return atomicpath;
}
