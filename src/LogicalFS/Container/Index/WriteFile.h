#ifndef __WriteFile_H__
#define __WriteFile_H__

#include "COPYRIGHT.h"
#include <map>
using namespace std;
#include "Util.h"
#include "Index.h"
#include "Metadata.h"

// THREAD SAFETY
// we use a mutex when writers are added or removed
// - the data_mux protects the fds map
// we do lookups into the fds map on writes
// but we assume that either FUSE is using us and it won't remove any writers
// until the release which means that everyone has done a close
// or that ADIO is using us and there won't be concurrency

// if we get multiple writers, we create an index mutex to protect it

struct
OpenFh {
    IOSHandle *fh;
    // Now that fh may not be opened during container_open, we track openFh
    // reference count in WriteFile::fhs_writers. OpenFh structure is still
    // preserved as an abstraction over iostore fh.
//  int writers;
};

class WriteFile : public Metadata
{
    public:
        WriteFile(string, string, mode_t, size_t index_buffer_mbs, pid_t,
                  string, struct plfs_backend *, PlfsMount *);
        ~WriteFile();

        plfs_error_t openIndex( pid_t );
        plfs_error_t closeIndex();

        plfs_error_t addPrepareWriter(pid_t pid, mode_t xmode, bool for_open,
                                      bool defer_open, const string &xbnode,
                                      PlfsMount *mntpt, const string &canbpath,
                                      struct plfs_backend *xcanback,
                                      int *ret_num_writers);

        plfs_error_t removeWriter( pid_t, int * );
        size_t numWriters();
        size_t maxWriters() {
            return max_writers;
        }

        plfs_error_t truncate( off_t );
        plfs_error_t extend( off_t );

        plfs_error_t write( const char *, size_t, off_t, pid_t, ssize_t * );

        plfs_error_t sync( );
        plfs_error_t sync( pid_t pid );

        void setContainerPath(string path);
        void setSubdirPath (string path, struct plfs_backend *wrback);

        plfs_error_t restoreFds(bool droppings_were_truncd);
        Index *getIndex() {
            return index;
        }
        void setPhysPath(struct plfs_physpathinfo *ppip_to);

        double createTime() {
            return createtime;
        }

    private:
        plfs_error_t addWriter( pid_t, bool, bool, int& );
        plfs_error_t openIndexFile( string path, string host, pid_t, mode_t,
                                    string *index_path, IOSHandle **ret_hand);
        plfs_error_t openDataFile(string path,string host,pid_t,mode_t,IOSHandle **ret_hand );
        plfs_error_t openFile( string, mode_t mode, IOSHandle **ret_hand );
        plfs_error_t Close( );
        plfs_error_t closeFh( IOSHandle *fh );
        struct OpenFh *getFh( pid_t pid );
        plfs_error_t prepareForWrite( pid_t pid );
        plfs_error_t prepareForWrite( ) {
            return prepareForWrite( open_pid );
        }

        pid_t open_pid;
        /*
         * XXX: it would be nice if we could minimize the amount of
         * path information we are caching in this object... this has
         * gotten fat due to the introduction of prepareWriter.
         */
        string bnode;           /* path without any mountpoint info */
        string container_path;  /* path to container (canonical?) */
        struct plfs_backend *canback;  /* canonical backend (XXX) */
        string subdir_path;     /* path to subdir our droppings live on */
        struct plfs_backend *subdirback;   /* dropping backend */
        PlfsMount *wrpmnt;      /* mount we are writing to */
        string hostname;
        map< pid_t, OpenFh  > fhs;
        map< pid_t, int > fhs_writers;
        // need to remember fd paths to restore
        map< IOSHandle *, string > paths;
        pthread_mutex_t    index_mux;  // to use the shared index
        pthread_mutex_t    data_mux;   // to access our map of fds
        bool has_been_renamed; // use this to guard against a truncate following
        // a rename
        size_t index_buffer_mbs;
        Index *index;
        mode_t mode;
        double createtime;
        size_t max_writers;
        // Keeps track of writes for flush of index
        int write_count;
};

#endif
