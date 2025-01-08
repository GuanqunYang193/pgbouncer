#include <usual/statlist.h>

#include <pthread.h>
#include <event2/event.h>
#include <event2/event_struct.h>

#define THREAD_NUM 4


#define FOR_EACH_THREAD(id) \
	for ((id) = 0; \
	     (id) < THREAD_NUM; \
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

    struct StatList sock_list;
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
    
} Thread;

typedef struct ClientRequest {
    int fd;
    bool is_unix;
} ClientRequest;

Thread threads[THREAD_NUM];
extern int next_thread;

void signal_setup(struct event_base * base, struct SignalEvent* signal_event);
void start_threads();
void init_threads();
void clean_threads();