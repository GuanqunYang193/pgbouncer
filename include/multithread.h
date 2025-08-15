#include <usual/statlist.h>
#include <usual/statlist_ts.h>
#include <usual/aatree.h>
#include <usual/spinlock.h>
#include <usual/time.h>
#include "bouncer.h"

#include <pthread.h>
#include <event2/event.h>
#include <event2/event_struct.h>

#define FOR_EACH_THREAD(id)         \
	for (int id = 0;                \
	     (id) < arg_thread_number;  \
	     (id)++)                    


#define GET_MULTITHREAD_LIST_PTR(name, thread_id) \
	(multithread_mode ? (void *)&(threads[thread_id].name) : (void *)&name)

#define GET_MULTITHREAD_CACHE_PTR(name, thread_id) \
	(multithread_mode ? (threads[thread_id].name) : (name))


#define MULTITHREAD_VISIT(multithread_mode, lock, func) 		        \
	do { 											                    \
		if (multithread_mode) { 					                    \
			spin_lock_acquire(lock); 			                        \
			func; 									                    \
			spin_lock_release(lock); 			                        \
		} else { 									                    \
			func; 									                    \
		}                                                               \
	} while (0)


typedef struct SignalEvent{
    /*
 * signal handling.
 *
 * handle_* functions are not actual signal handlers but called from
 * event_loop() so they have no restrictions what they can do.
 */
 struct event ev_sigterm;
 struct event ev_sigint;

#ifndef WIN32

 struct event ev_sigquit;
 struct event ev_sigusr1;
 struct event ev_sigusr2;
 struct event ev_sighup;
#endif
} SignalEvent;


typedef struct WorkdersignalEvents{

    int pipe_sigterm[2];
    int pipe_sigint[2];
    struct event* ev_sigterm;
    struct event* ev_sigint;

#ifndef WIN32
    int pipe_sigquit[2];
    int pipe_sigusr1[2];
    int pipe_sigusr2[2];
    int pipe_sighup[2];
    struct event* ev_sigquit;
    struct event* ev_sigusr1;
    struct event* ev_sigusr2;
    struct event* ev_sighup;
#endif

} WorkdersignalEvents;

enum ThreadStatus{
    THREAD_RUNNING,         // resumed
    THREAD_REQUEST_PAUSE,   // request sent but not paused
    THREAD_PAUSED,          // thread confirms paused
};

typedef struct ThreadMetadata{
    enum ThreadStatus thread_status;
    SpinLock thread_lock;
} ThreadMetadata;

#define THREAD_PAUSE_SEC 0.5

typedef struct Thread {
    ThreadMetadata thread_metadata;
    pthread_t worker;
    int thread_id;
    struct event full_maint_ev;
    struct event ev_stats;
    struct event ev_handle_request;
    int pipefd[2];
    struct StatList login_client_list;
    struct ThreadSafeStatList pool_list;
    struct StatList peer_pool_list;
    struct WorkdersignalEvents worker_signal_events;
    struct ThreadSafeStatList database_list;
    struct StatList autodatabase_idle_list;
    struct Slab *client_cache;
    struct Slab *server_cache;
    struct Slab *pool_cache;
    struct Slab *peer_pool_cache;
    struct Slab *db_cache;
    struct Slab *var_list_cache;
    struct Slab *iobuf_cache;
    struct Slab *server_prepared_statement_cache;
    struct Slab *outstanding_request_cache;
    
    /* All locally defined users (in auth_file) are kept here. */
    struct AATree user_tree;

    /*
    * All PAM users are kept here. We need to differentiate two user
    * lists to avoid user clashing for different authentication types,
    * and because pam_user_tree is closer to PgDatabase.user_tree in
    * logic.
    */
    struct AATree pam_user_tree;

    /*
    * libevent may still report events when event_del()
    * is called from somewhere else.  So hide just freed
    * PgSockets for one loop.
    */
    struct StatList justfree_client_list;
    struct StatList justfree_server_list;

    struct StrPool *vpool;

    struct PktBuf *temp_pktbuf;

    int cf_shutdown;

    unsigned int seq;

    PgStats cur_stat;
    SpinLock cur_stat_lock;
    usec_t multithread_time_cache;
} Thread;

typedef struct ClientRequest {
    int fd;
    bool is_unix;
} ClientRequest;


extern Thread *threads;
extern int next_thread;

void signal_setup(struct event_base * base, struct SignalEvent* signal_event);
void start_threads(void);
void init_threads(void);
int wait_threads(void);
void clean_threads(void);
void request_pause_thread(int thread_id);
bool thread_paused(int thread_id);
void resume_thread(int thread_id);
void lock_thread(int thread_id);
void unlock_thread(int thread_id);
void lock_and_pause_thread(int thread_id);
void unlock_and_resume_thread(int thread_id);

void set_thread_id(int thread_id);
int get_current_thread_id(const bool multithread_mode);

usec_t get_multithread_time(void);
usec_t get_multithread_time_with_id(int thread_id);

void multithread_reset_time_cache(void);
