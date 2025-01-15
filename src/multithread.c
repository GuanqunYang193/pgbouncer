#include <multithread.h>
#include <bouncer.h>
#include <pooler.h>

int next_thread = 0;

void handle_sigterm(evutil_socket_t sock, short flags, void *arg)
{
	if (cf_shutdown) {
		log_info("got SIGTERM while shutting down, fast exit");
		/* pidfile cleanup happens via atexit() */
		exit(0);
	}
	log_info("got SIGTERM, shutting down, waiting for all clients disconnect");
	sd_notify(0, "STOPPING=1");
	if (cf_reboot)
		die("takeover was in progress, going down immediately");
	if (cf_pause_mode == P_SUSPEND)
		die("suspend was in progress, going down immediately");
	cf_shutdown = SHUTDOWN_WAIT_FOR_CLIENTS;
	cleanup_sockets();
}


static void handle_sigint(evutil_socket_t sock, short flags, void *arg)
{
	if (cf_shutdown) {
		log_info("got SIGINT while shutting down, fast exit");
		/* pidfile cleanup happens via atexit() */
		exit(0);
	}
	log_info("got SIGINT, shutting down, waiting for all servers connections to be released");
	sd_notify(0, "STOPPING=1");
	if (cf_reboot)
		die("takeover was in progress, going down immediately");
	if (cf_pause_mode == P_SUSPEND)
		die("suspend was in progress, going down immediately");
	cf_pause_mode = P_PAUSE;
	cf_shutdown = SHUTDOWN_WAIT_FOR_SERVERS;
	cleanup_sockets();
}




#ifndef WIN32

static void handle_sigquit(evutil_socket_t sock, short flags, void *arg)
{
	log_info("got SIGQUIT, fast exit");
	/* pidfile cleanup happens via atexit() */
	exit(0);
}

static void handle_sigusr1(int sock, short flags, void *arg)
{
	if (cf_pause_mode == P_NONE) {
		log_info("got SIGUSR1, pausing all activity");
		cf_pause_mode = P_PAUSE;
	} else {
		log_info("got SIGUSR1, but already paused/suspended");
	}
}

static void handle_sigusr2(int sock, short flags, void *arg)
{
	if (cf_shutdown) {
		log_info("got SIGUSR2 while shutting down, ignoring");
		return;
	}
	switch (cf_pause_mode) {
	case P_SUSPEND:
		log_info("got SIGUSR2, continuing from SUSPEND");
		resume_all();
		cf_pause_mode = P_NONE;
		break;
	case P_PAUSE:
		log_info("got SIGUSR2, continuing from PAUSE");
		cf_pause_mode = P_NONE;
		break;
	case P_NONE:
		log_info("got SIGUSR2, but not paused/suspended");
	}
}


/*
 * Notify systemd that we are reloading, including a CLOCK_MONOTONIC timestamp
 * in usec so that the program is compatible with a Type=notify-reload service.
 *
 * See https://www.freedesktop.org/software/systemd/man/latest/sd_notify.html
 */
static void notify_reloading(void)
{
#ifdef USE_SYSTEMD
	struct timespec ts;
	usec_t usec;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	usec = (usec_t)ts.tv_sec * USEC + (usec_t)ts.tv_nsec / (usec_t)1000;
	sd_notifyf(0, "RELOADING=1\nMONOTONIC_USEC=%" PRIu64, usec);
#endif
}

static void handle_sighup(int sock, short flags, void *arg)
{
	log_info("got SIGHUP, re-reading config");
	notify_reloading();
	load_config();
	if (!sbuf_tls_setup())
		log_error("TLS configuration could not be reloaded, keeping old configuration");
	sd_notify(0, "READY=1");
}
#endif


void signal_setup(struct event_base * base, struct SignalEvent* signal_event)
{
	int err;

#ifndef WIN32
	sigset_t set;

	/* block SIGPIPE */
	sigemptyset(&set);
	sigaddset(&set, SIGPIPE);
	err = sigprocmask(SIG_BLOCK, &set, NULL);
	if (err < 0)
		fatal_perror("sigprocmask");

	/* install handlers */

	evsignal_assign(&(signal_event->ev_sigusr1), base, SIGUSR1, handle_sigusr1, NULL);
	err = evsignal_add(&(signal_event->ev_sigusr1), NULL);
	if (err < 0)
		fatal_perror("evsignal_add");

	evsignal_assign(&(signal_event->ev_sigusr2), base, SIGUSR2, handle_sigusr2, NULL);
	err = evsignal_add(&(signal_event->ev_sigusr2), NULL);
	if (err < 0)
		fatal_perror("evsignal_add");

	evsignal_assign(&(signal_event->ev_sighup), base, SIGHUP, handle_sighup, NULL);
	err = evsignal_add(&(signal_event->ev_sighup), NULL);
	if (err < 0)
		fatal_perror("evsignal_add");

	evsignal_assign(&(signal_event->ev_sigquit), base, SIGQUIT, handle_sigquit, NULL);
	err = evsignal_add(&(signal_event->ev_sigquit), NULL);
	if (err < 0)
		fatal_perror("evsignal_add");
#endif
	evsignal_assign(&(signal_event->ev_sigterm), base, SIGTERM, handle_sigterm, NULL);
	err = evsignal_add(&(signal_event->ev_sigterm), NULL);
	if (err < 0)
		fatal_perror("evsignal_add");

	evsignal_assign(&(signal_event->ev_sigint), base, SIGINT, handle_sigint, NULL);
	err = evsignal_add(&(signal_event->ev_sigint), NULL);
	if (err < 0)
		fatal_perror("evsignal_add");
}


void* worker_func(void* arg){
    
    Thread * this_thread = (Thread*) arg;
    pthread_setspecific(thread_pointer, this_thread);

    struct event_base *base = event_base_new();
    if (!base) {
        fprintf(stderr, "[Thread %ld] Failed to create event_base.\n", this_thread->thread_id);
        die("event_base_new() failed");
    }

    pthread_setspecific(event_base_key, base);

	admin_setup();
    thread_pooler_setup();
	signal_setup(base, &(this_thread->signal_event));
	janitor_setup();
	stats_setup();

    while(true){
        int err;
        reset_time_cache();
        err = event_base_loop(base, EVLOOP_ONCE);
        if (err < 0) {
            if (errno != EINTR)
                log_warning("event_loop failed: %s", strerror(errno));
        }
        per_loop_maint();
        reuse_just_freed_objects();
        rescue_timers();
        per_loop_pooler_maint();
    }
    return NULL;
}

static void event_base_destructor(void* base_ptr) {
    if (base_ptr) {
        printf("[Destructor] Free event_base: %p\n", base_ptr);
        event_base_free((struct event_base*)base_ptr);
    }
}

void init_thread(int thread_id){
	threads[thread_id].thread_id = thread_id;
	if (pipe(threads[thread_id].pipefd) < 0) {
		die("Thread %ld init failed",thread_id);
	}
	int flags = fcntl(threads[thread_id].pipefd[1], F_GETFL, 0);
	if (fcntl(threads[thread_id].pipefd[1], F_SETFL, flags | O_NONBLOCK) < 0) {
		die("set pipe flag failed");
	}
	statlist_init(&(threads[thread_id].sock_list), NULL);
	statlist_init(&(threads[thread_id].pool_list), NULL);
	statlist_init(&(threads[thread_id].peer_pool_list), NULL);
	statlist_init(&(threads[thread_id].login_client_list), NULL);
	statlist_init(&(threads[thread_id].database_list), NULL);
	statlist_init(&(threads[thread_id].autodatabase_idle_list), NULL);
	statlist_init(&(threads[thread_id].user_list), NULL);
	statlist_init(&(threads[thread_id].justfree_client_list), NULL);
	statlist_init(&(threads[thread_id].justfree_server_list), NULL);
}

void start_threads(){
	pthread_key_create(&event_base_key, event_base_destructor);
    pthread_key_create(&thread_pointer, NULL);
	int thread_id;
	FOR_EACH_THREAD(thread_id){	
		pthread_create(&threads[thread_id].worker, NULL, worker_func, &threads[thread_id]);
	}
	// TODO wait until threads ready
	
}

void init_threads(){
	for(int i=0;i<THREAD_NUM;i++){
		init_thread(i);
	}
}