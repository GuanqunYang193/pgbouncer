#include <usual/statlist.h>
#include <usual/aatree.h>

#include <pthread.h>
#include <event2/event.h>
#include <event2/event_struct.h>


#define FOR_EACH_THREAD(id)         \
	for (int id = 0;                \
	     (id) < arg_thread_number;   \
	     (id)++)                    

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

typedef struct Thread {

    pthread_t worker;
    int thread_id;
    struct event full_maint_ev;
    struct event ev_stats;
    struct event ev_handle_request;
    int pipefd[2];
    struct StatList login_client_list;
    struct StatList pool_list;
    struct StatList peer_pool_list;
    struct SignalEvent signal_event;
    struct StatList database_list;
    struct StatList autodatabase_idle_list;
    struct StatList user_list;
    struct Slab *client_cache;
    struct Slab *user_cache;
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
} Thread;

typedef struct ClientRequest {
    int fd;
    bool is_unix;
} ClientRequest;

Thread *threads;
extern int next_thread;

void signal_setup(struct event_base * base, struct SignalEvent* signal_event, int thread_id);
void start_threads();
void init_threads();
int wait_threads();
void clean_threads();