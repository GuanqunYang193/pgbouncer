/*
 * PgBouncer - Lightweight connection pooler for PostgreSQL.
 *
 * Copyright (c) 2007-2009  Marko Kreen, Skype Technologies OÜ
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

/*
 * Periodic maintenance.
 */

#include "bouncer.h"
#include "multithread.h"

#include <usual/slab.h>
#include <usual/slab_ts.h>


/* do full maintenance 3x per second */
static struct timeval full_maint_period = {0, USEC / 3};
static struct event full_maint_ev;

extern bool any_user_level_server_timeout_set;
extern bool any_user_level_client_timeout_set;

/* close all sockets in server list */
static void close_server_list(struct StatList *sk_list, const char *reason, int thread_id)
{
	struct List *item, *tmp;
	PgSocket *server;

	statlist_for_each_safe(item, sk_list, tmp) {
		server = container_of(item, PgSocket, head);
		disconnect_server(server, true, thread_id, "%s", reason);
	}
}

static void close_client_list(struct StatList *sk_list, const char *reason, int thread_id)
{
	struct List *item, *tmp;
	PgSocket *client;

	statlist_for_each_safe(item, sk_list, tmp) {
		client = container_of(item, PgSocket, head);
		disconnect_client(client, true, thread_id, "%s", reason);
	}
}

bool suspend_socket(PgSocket *sk, bool force_suspend, int thread_id)
{
	if (sk->suspended)
		return true;

	if (sbuf_is_empty(&sk->sbuf)) {
		if (sbuf_pause(&sk->sbuf))
			sk->suspended = true;
	}

	if (sk->suspended || !force_suspend)
		return sk->suspended;

	if (is_server_socket(sk))
		disconnect_server(sk, true, thread_id, "suspend_timeout");
	else
		disconnect_client(sk, true, thread_id, "suspend_timeout");
	return true;
}

/* suspend all sockets in socket list */
static int suspend_socket_list(struct StatList *list, bool force_suspend, int thread_id)
{
	struct List *item, *tmp;
	PgSocket *sk;
	int active = 0;

	statlist_for_each_safe(item, list, tmp) {
		sk = container_of(item, PgSocket, head);
		if (!suspend_socket(sk, force_suspend, thread_id))
			active++;
	}
	return active;
}

/* resume all suspended sockets in socket list */
static void resume_socket_list(struct StatList *list)
{
	struct List *item, *tmp;
	PgSocket *sk;

	statlist_for_each_safe(item, list, tmp) {
		sk = container_of(item, PgSocket, head);
		if (sk->suspended) {
			sk->suspended = false;
			sbuf_continue(&sk->sbuf);
		}
	}
}

static void resume_sockets_cb(struct List *item, void *ctx){
	PgPool *pool = container_of(item, PgPool, head);
	if (pool->db->admin)
		return;
	resume_socket_list(&pool->active_client_list);
	resume_socket_list(&pool->active_server_list);
	resume_socket_list(&pool->idle_server_list);
	resume_socket_list(&pool->used_server_list);
}

/* resume all suspended sockets in all pools */
static void resume_sockets(void)
{
	struct List *item;
	PgPool *pool;
	if(multithread_mode){
		FOR_EACH_THREAD(thread_id){
			log_info("resume_sockets thread_id: %d", thread_id);
			thread_safe_statlist_iterate(&(threads[thread_id].pool_list), resume_sockets_cb, NULL);
		}
	}else{
		statlist_for_each(item, &pool_list) {
			pool = container_of(item, PgPool, head);
			if (pool->db->admin)
				continue;
			resume_socket_list(&pool->active_client_list);
			resume_socket_list(&pool->active_server_list);
			resume_socket_list(&pool->idle_server_list);
			resume_socket_list(&pool->used_server_list);
		}
	}

}
/* resume pools and listen sockets */
void resume_all(void)
{
	resume_sockets();
	resume_pooler();
}

/*
 * send test/reset query to server if needed
 */
static void launch_recheck(PgPool *pool, int thread_id)
{
	const char *q = cf_server_check_query;
	bool need_check = true;
	PgSocket *server;
	bool res = true;

	/* find clean server */
	while (1) {
		server = first_socket(&pool->used_server_list);
		if (!server)
			return;
		if (server->ready)
			break;
		disconnect_server(server, true, thread_id, "idle server got dirty");
	}

	/* is the check needed? */
	if (q == NULL || q[0] == 0) {
		need_check = false;
	} else if (cf_server_check_delay > 0) {
		usec_t now = get_multithread_time_with_id(thread_id);
		if (now - server->request_time < cf_server_check_delay)
			need_check = false;
	}

	if (need_check) {
		/* send test query, wait for result */
		slog_debug(server, "P: checking: %s", q);
		change_server_state(server, SV_TESTED, thread_id);
		if (empty_server_check_query)
			SEND_generic(res, server, PqMsg_Query, "s", "\0");
		else
			SEND_generic(res, server, PqMsg_Query, "s", q);
		if (!res)
			disconnect_server(server, false, thread_id, "test query failed");
	} else {
		/* make immediately available */
		release_server(server, thread_id);
	}
}

/*
 * make servers available
 */
static void per_loop_activate(PgPool *pool, int thread_id)
{
	struct List *item, *tmp;
	PgSocket *client;
	int sv_tested, sv_used;

	/* if there is a cancel request waiting, open a new connection */
	if (!statlist_empty(&pool->waiting_cancel_req_list)) {
		launch_new_connection(pool, /* evict_if_needed= */ true, thread_id);
		return;
	}

	/* see if any server have been freed */
	sv_tested = statlist_count(&pool->tested_server_list);
	sv_used = statlist_count(&pool->used_server_list);
	statlist_for_each_safe(item, &pool->waiting_client_list, tmp) {
		PktBuf *buf;
		bool res;
		usec_t time = get_multithread_time_with_id(thread_id);
		client = container_of(item, PgSocket, head);
		if (client->state == CL_WAITING
		    && !client->sent_wait_notification
		    && client->welcome_sent
		    && ((time - client->wait_start) / USEC) > cf_query_wait_notify
		    && cf_query_wait_notify > 0) {
			buf = pktbuf_dynamic(256);
			pktbuf_write_Notice(
				buf,
				"No server connection available in postgres backend, client being queued"
				);
			res = pktbuf_send_queued(buf, client);
			if (!res)
				log_warning("Sending queue warning failed");
			client->sent_wait_notification = true;
		}

		if (client->replication) {
			/*
			 * For replication connections we always launch
			 * a new connection, but we continue with the loop,
			 * because there might be normal clients waiting too.
			 */
			launch_new_connection(pool, /* evict_if_needed= */ true, thread_id);
		} else if (!statlist_empty(&pool->idle_server_list)) {
			/* db not fully initialized after reboot */
			if (client->wait_for_welcome && !pool->welcome_msg_ready) {
				launch_new_connection(pool, /* evict_if_needed= */ true, thread_id);
				continue;
			}

			/* there is a ready server already */
			activate_client(client, thread_id);
		} else if (sv_tested > 0) {
			/* some connections are in testing process */
			--sv_tested;
		} else if (sv_used > 0) {
			/* ask for more connections to be tested */
			launch_recheck(pool, thread_id);
			--sv_used;
		} else {
			/* not enough connections */
			launch_new_connection(pool, /* evict_if_needed= */ true, thread_id);
			break;
		}
	}
}

/*
 * pause active clients
 */
static int per_loop_pause(PgPool *pool, int thread_id)
{
	int active = 0;

	if (pool->db->admin)
		return 0;

	close_server_list(&pool->idle_server_list, "pause mode", thread_id);
	close_server_list(&pool->used_server_list, "pause mode", thread_id);
	close_server_list(&pool->new_server_list, "pause mode", thread_id);

	active += statlist_count(&pool->active_server_list);
	active += statlist_count(&pool->tested_server_list);

	return active;
}

/*
 * suspend active clients and servers
 */
static int per_loop_suspend(PgPool *pool, bool force_suspend, int thread_id)
{
	int active = 0;

	if (pool->db->admin)
		return 0;

	active += suspend_socket_list(&pool->active_client_list, force_suspend, thread_id);

	/* this list is not suspendable, but still need force_suspend and counting */
	active += suspend_socket_list(&pool->waiting_client_list, force_suspend, thread_id);
	if (active)
		per_loop_activate(pool, thread_id);

	if (!active) {
		active += suspend_socket_list(&pool->active_server_list, force_suspend, thread_id);
		active += suspend_socket_list(&pool->idle_server_list, force_suspend, thread_id);

		/* as all clients are done, no need for them */
		close_server_list(&pool->tested_server_list, "close unsafe file descriptors on suspend", thread_id);
		close_server_list(&pool->used_server_list, "close unsafe file descriptors on suspend", thread_id);
	}

	return active;
}

/*
 * Count the servers in server_list that have close_needed set.
 */
static int count_close_needed(struct StatList *server_list)
{
	struct List *item;
	PgSocket *server;
	int count = 0;

	statlist_for_each(item, server_list) {
		server = container_of(item, PgSocket, head);
		if (server->close_needed)
			count++;
	}

	return count;
}

/*
 * Per-loop tasks for WAIT_CLOSE
 */
static int per_loop_wait_close(PgPool *pool)
{
	int count = 0;

	if (pool->db->admin)
		return 0;

	count += count_close_needed(&pool->active_server_list);
	count += count_close_needed(&pool->idle_server_list);
	count += count_close_needed(&pool->new_server_list);
	count += count_close_needed(&pool->tested_server_list);
	count += count_close_needed(&pool->used_server_list);

	return count;
}

static void per_loop_maint_cb(struct List *item, void *ctx) {
	PgPool *pool;
	
	struct {
		int *active_count_ptr;
		int *waiting_count_ptr;
		bool *partial_pause_ptr;
		bool *partial_wait_ptr;
		bool *force_suspend_ptr;
		int thread_id;
	} *data = ctx;

	pool = container_of(item, PgPool, head);

	if (pool->db->admin)
		return;

	switch (cf_pause_mode) {
	case P_NONE:
		if (pool->db->db_paused) {
			*data->partial_pause_ptr = true;
			*data->active_count_ptr += per_loop_pause(pool, data->thread_id);
		} else {
			per_loop_activate(pool, data->thread_id);
		}
		break;
	case P_PAUSE:
		*data->active_count_ptr += per_loop_pause(pool, data->thread_id);
		break;
	case P_SUSPEND:
		*data->active_count_ptr += per_loop_suspend(pool, *data->force_suspend_ptr, data->thread_id);
		break;
	}

	if (pool->db->db_wait_close) {
		*data->partial_wait_ptr = true;
		*data->waiting_count_ptr += per_loop_wait_close(pool);
	}
}

/*
 * this function is called for each event loop.
 */
void per_loop_maint(void)
{
	struct List *item;
	PgPool *pool;
	int thread_id;
	void* pool_list_ptr;
	struct StatList* login_client_list_;
	int thread_pause_mode;
	int active_count = 0;
	int waiting_count = 0;
	bool partial_pause = false;
	bool partial_wait = false;
	bool force_suspend = false;
	thread_id = get_current_thread_id(multithread_mode);
	
	if (cf_pause_mode == P_SUSPEND && cf_suspend_timeout > 0) {
		usec_t stime = get_multithread_time_with_id(thread_id) - g_suspend_start;
		if (stime >= cf_suspend_timeout)
			force_suspend = true;
	}

	pool_list_ptr = GET_MULTITHREAD_LIST_PTR(pool_list, thread_id);

	if(multithread_mode){
		struct {
			int *active_count_ptr;
			int *waiting_count_ptr;
			bool *partial_pause_ptr;
			bool *partial_wait_ptr;
			bool *force_suspend_ptr;
			int thread_id;
		} ctx = { &active_count, &waiting_count, &partial_pause, &partial_wait, &force_suspend, thread_id};

		thread_safe_statlist_iterate((struct ThreadSafeStatList*)pool_list_ptr, per_loop_maint_cb, &ctx);
		
		/* Store partial_pause state in thread structure */
		threads[thread_id].partial_pause = partial_pause;
	} else {
		statlist_for_each(item, (struct StatList*)pool_list_ptr) {
			pool = container_of(item, PgPool, head);
			if (pool->db->admin)
				continue;
			switch (cf_pause_mode) {
			case P_NONE:
				if (pool->db->db_paused) {
					partial_pause = true;
					active_count += per_loop_pause(pool, thread_id);
				} else {
					per_loop_activate(pool, thread_id);
				}
				break;
			case P_PAUSE:
				active_count += per_loop_pause(pool, thread_id);
				break;
			case P_SUSPEND:
				active_count += per_loop_suspend(pool, force_suspend, thread_id);
				break;
			}

			if (pool->db->db_wait_close) {
				partial_wait = true;
				waiting_count += per_loop_wait_close(pool);
			}
		}
	}

	login_client_list_ = GET_MULTITHREAD_LIST_PTR(login_client_list, thread_id);

	if (multithread_mode) {
		int thread_id = get_current_thread_id(multithread_mode);
		thread_pause_mode = threads[thread_id].cf_pause_mode;
	} else {
		thread_pause_mode = cf_pause_mode;
	}

	switch (thread_pause_mode) {
	case P_SUSPEND:
		if (force_suspend) {
			close_client_list(login_client_list_, "suspend_timeout", thread_id);
		} else {
			active_count = statlist_count(login_client_list_);
			/* Store active count in thread structure for multithread coordination */
			if (multithread_mode) {
				int thread_id = get_current_thread_id(multithread_mode);
				threads[thread_id].active_count = active_count;
				MULTITHREAD_VISIT(multithread_mode, &total_active_count_lock, {
					total_active_count += active_count;
				});
			}
		}
	/* fallthrough */
	case P_PAUSE:
		if (!active_count) {
			if (multithread_mode) {
				int thread_id = get_current_thread_id(multithread_mode);
				threads[thread_id].pause_ready = true;
			} else {
				admin_pause_done();
			}
		}
		break;
	case P_NONE:
		if (partial_pause && !active_count) {
			if (multithread_mode) {
				int thread_id = get_current_thread_id(multithread_mode);
				threads[thread_id].pause_ready = true;
			} else {
				admin_pause_done();
			}
		}
		break;
	}

	if (partial_wait && !waiting_count) {
		if (multithread_mode) {
			int thread_id = get_current_thread_id(multithread_mode);
			threads[thread_id].wait_close_ready = true;
		} else {
			admin_wait_close_done();
		}
	}
}

void per_loop_admin_condition_maint(void)
{
	bool any_pause_mode = false;
	bool any_suspend_mode = false;
	bool all_wait_close_ready = true;
	
	/* Check if all threads are ready for pause or suspend */
	FOR_EACH_THREAD(thread_id) {
		if (threads[thread_id].cf_pause_mode == P_PAUSE || 
		    (threads[thread_id].cf_pause_mode == P_NONE && threads[thread_id].partial_pause)) {
			any_pause_mode = true;
		}
		if (threads[thread_id].cf_pause_mode == P_SUSPEND) {
			any_suspend_mode = true;
		}
	}
	
	/* Handle PAUSE mode */
	if (any_pause_mode) {
		bool all_threads_ready = true;
		FOR_EACH_THREAD(thread_id) {
			if (!threads[thread_id].pause_ready) {
				all_threads_ready = false;
				break;
			}
		}
		
		if (all_threads_ready) {
			/* All threads are ready, send admin response */
			admin_pause_done();
			
			/* Reset flags */
			FOR_EACH_THREAD(thread_id) {
				threads[thread_id].pause_ready = false;
			}
		}
	}
	
	/* Handle SUSPEND mode */
	if (any_suspend_mode) {
		/* If total active count is 0, all threads are ready for suspend */
		if (total_active_count == 0) {
			/* All threads are ready, send admin response */
			admin_pause_done();
			
			/* Reset flags */
			FOR_EACH_THREAD(thread_id) {
				threads[thread_id].pause_ready = false;
			}
		}
	}
	
	/* Check if all threads are ready for wait_close */
	FOR_EACH_THREAD(thread_id) {
		if (!threads[thread_id].wait_close_ready) {
			all_wait_close_ready = false;
			break;
		}
	}
	
	if (all_wait_close_ready) {
		/* All threads are ready, send admin response */
		admin_wait_close_done();
		
		/* Reset flags */
		FOR_EACH_THREAD(thread_id) {
			threads[thread_id].wait_close_ready = false;
		}
	}
}


/* maintaining clients in pool */
static void pool_client_maint(PgPool *pool, int thread_id)
{
	struct List *item, *tmp;
	usec_t now = get_multithread_time_with_id(thread_id);
	PgSocket *client;
	PgGlobalUser *user;
	usec_t age;
	usec_t effective_client_idle_timeout;

	/* force client_idle_timeout */
	if (cf_client_idle_timeout > 0 || any_user_level_client_timeout_set) {
		statlist_for_each_safe(item, &pool->active_client_list, tmp) {
			client = container_of(item, PgSocket, head);
			Assert(client->state == CL_ACTIVE);
			if (client->link)
				continue;

			user = client->login_user_credentials->global_user;
			effective_client_idle_timeout = cf_client_idle_timeout;

			if (user->client_idle_timeout > 0)
				effective_client_idle_timeout = user->client_idle_timeout;

			if (now - client->request_time > effective_client_idle_timeout)
				disconnect_client(client, true, thread_id, "client_idle_timeout");
		}
	}


	/* force timeouts for waiting queries */
	if (cf_query_timeout > 0 || cf_query_wait_timeout > 0) {
		statlist_for_each_safe(item, &pool->waiting_client_list, tmp) {
			int* cf_shutdown_ptr;
			client = container_of(item, PgSocket, head);

			Assert(client->state == CL_WAITING || client->state == CL_WAITING_LOGIN);
			if (client->query_start == 0) {
				age = now - client->request_time;
				/* log_warning("query_start==0"); */
			} else {
				age = now - client->query_start;
			}
			
			if(multithread_mode){
				cf_shutdown_ptr = &(threads[thread_id].cf_shutdown);
			} else {
				cf_shutdown_ptr = &cf_shutdown;
			}
			if (*cf_shutdown_ptr == SHUTDOWN_WAIT_FOR_SERVERS) {
				disconnect_client(client, true, thread_id, "server shutting down");
			} else if (cf_query_timeout > 0 && age > cf_query_timeout) {
				disconnect_client(client, true, thread_id, "query_timeout");
			} else if (cf_query_wait_timeout > 0 && age > cf_query_wait_timeout) {
				disconnect_client(client, true, thread_id, "query_wait_timeout");
			}
		}
	}

	/* apply cancel_wait_timeout for cancel connections */
	if (cf_cancel_wait_timeout > 0) {
		statlist_for_each_safe(item, &pool->waiting_cancel_req_list, tmp) {
			client = container_of(item, PgSocket, head);
			Assert(client->state == CL_WAITING_CANCEL);
			age = now - client->request_time;

			if (age > cf_cancel_wait_timeout)
				disconnect_client(client, false, thread_id, "cancel_wait_timeout");
		}
	}

	/* apply client_login_timeout to clients waiting for welcome pkt */
	if (cf_client_login_timeout > 0 && !pool->welcome_msg_ready) {
		statlist_for_each_safe(item, &pool->waiting_client_list, tmp) {
			client = container_of(item, PgSocket, head);
			if (!client->wait_for_welcome)
				continue;
			age = now - client->connect_time;
			if (age > cf_client_login_timeout)
				disconnect_client(client, true, thread_id, "client_login_timeout (server down)");
		}
	}
}

/* maintaining clients in peer pool */
static void peer_pool_client_maint(PgPool *pool, int thread_id)
{
	struct List *item, *tmp;
	usec_t now = get_multithread_time_with_id(thread_id);
	PgSocket *client;
	usec_t age;

	if (cf_cancel_wait_timeout > 0) {
		statlist_for_each_safe(item, &pool->waiting_cancel_req_list, tmp) {
			client = container_of(item, PgSocket, head);
			Assert(client->state == CL_WAITING_CANCEL);
			age = now - client->request_time;

			if (age > cf_cancel_wait_timeout)
				disconnect_client(client, false, thread_id, "cancel_wait_timeout");
		}
	}
}

static void check_unused_servers(PgPool *pool, struct StatList *slist, bool idle_test, int thread_id)
{
	usec_t now = get_multithread_time_with_id(thread_id);
	usec_t server_lifetime = pool_server_lifetime(pool);

	struct List *item, *tmp;
	usec_t idle, age;
	PgSocket *server;

	/* disconnect idle servers if needed */
	statlist_for_each_safe(item, slist, tmp) {
		server = container_of(item, PgSocket, head);

		age = now - server->connect_time;
		idle = now - server->request_time;

		if (server->close_needed) {
			disconnect_server(server, true, thread_id, "database configuration changed");
		} else if (server->state == SV_IDLE && !server->ready) {
			disconnect_server(server, true, thread_id, "SV_IDLE server got dirty");
		} else if (server->state == SV_USED && !server->ready) {
			disconnect_server(server, true, thread_id, "SV_USED server got dirty");
		} else if (cf_server_idle_timeout > 0 && idle > cf_server_idle_timeout
			   && (pool_min_pool_size(pool) == 0 || pool_connected_server_count(pool) > pool_min_pool_size(pool))) {
			disconnect_server(server, true, thread_id, "server idle timeout");
		} else if (age >= server_lifetime) {
			if (life_over(server, thread_id)) {
				disconnect_server(server, true, thread_id, "server lifetime over");
				pool->last_lifetime_disconnect = now;
			}
		} else if (cf_pause_mode == P_PAUSE) {
			disconnect_server(server, true, thread_id, "pause mode");
		} else if (idle_test && *cf_server_check_query) {
			if (idle > cf_server_check_delay)
				change_server_state(server, SV_USED, thread_id);
		}
	}
}

/*
 * Check pool size, close conns if too many.  Makes pooler
 * react faster to the case when admin decreased pool size.
 */
static void check_pool_size(PgPool *pool, int thread_id)
{
	PgSocket *server;
	int cur = pool_connected_server_count(pool);
	int many = cur - (pool_pool_size(pool) + pool_res_pool_size(pool));

	Assert(pool_pool_size(pool) >= 0);

	if (pool_pool_size(pool) > 0) {
		while (many > 0) {
			server = first_socket(&pool->used_server_list);
			if (!server)
				server = first_socket(&pool->idle_server_list);
			if (!server)
				break;
			disconnect_server(server, true, thread_id, "too many servers in the pool");
			many--;
			cur--;
		}
	}

	/* launch extra connections to satisfy min_pool_size */
	if (cur < pool_min_pool_size(pool) &&
	    cur < pool_pool_size(pool) &&
	    cf_pause_mode == P_NONE &&
	    cf_reboot == 0 &&
	    (pool_client_count(pool) > 0 || pool->db->forced_user_credentials != NULL)) {
		log_debug("launching new connection to satisfy min_pool_size");
		launch_new_connection(pool, /* evict_if_needed= */ false, thread_id);
	}
}

/* maintain servers in a pool */
static void pool_server_maint(PgPool *pool, int thread_id)
{
	struct List *item, *tmp;
	usec_t now = get_multithread_time_with_id(thread_id);
	PgSocket *server;

	/* find and disconnect idle servers */
	check_unused_servers(pool, &pool->used_server_list, 0, thread_id);
	check_unused_servers(pool, &pool->tested_server_list, 0, thread_id);
	check_unused_servers(pool, &pool->idle_server_list, 1, thread_id);

	statlist_for_each_safe(item, &pool->active_server_list, tmp) {
		server = container_of(item, PgSocket, head);
		Assert(server->state == SV_ACTIVE);
		/*
		 * Disconnect active servers without outstanding requests if
		 * server_fast_close is set. This only applies to session
		 * pooling.
		 */
		if (cf_server_fast_close && server->ready && server->close_needed)
			disconnect_server(server, true, thread_id, "database configuration changed");
		/*
		 * Always disconnect close_needed replication servers. These
		 * connections are expected to be very long lived (possibly
		 * indefinitely), so waiting until the session/transaction is
		 * over is not an option.
		 */
		if (server->replication && server->close_needed)
			disconnect_server(server, true, thread_id, "database configuration changed");
	}

	/* handle query_timeout and idle_transaction_timeout */
	if (cf_query_timeout > 0 || cf_idle_transaction_timeout > 0 || cf_transaction_timeout > 0 || any_user_level_timeout_set) {
		statlist_for_each_safe(item, &pool->active_server_list, tmp) {
			usec_t age_client, age_server, age_transaction;
			usec_t effective_query_timeout;
			usec_t effective_idle_transaction_timeout;
			usec_t user_query_timeout;
			usec_t user_idle_transaction_timeout;
			usec_t user_transaction_timeout;
			usec_t effective_transaction_timeout;

			server = container_of(item, PgSocket, head);
			Assert(server->state == SV_ACTIVE);
			if (server->ready)
				continue;

			/*
			 * Note the different age calculations:
			 * query_timeout counts from the last request
			 * of the client (the client started the
			 * query), idle_transaction_timeout counts
			 * from the last request of the server (the
			 * server sent the idle information).
			 */
			age_client = now - server->link->request_time;
			age_server = now - server->request_time;
			age_transaction = now - server->link->xact_start;

			user_idle_transaction_timeout = server->login_user_credentials->global_user->idle_transaction_timeout;
			user_transaction_timeout = server->login_user_credentials->global_user->transaction_timeout;
			user_query_timeout = server->login_user_credentials->global_user->query_timeout;

			effective_idle_transaction_timeout = cf_idle_transaction_timeout;
			effective_query_timeout = cf_query_timeout;
			effective_transaction_timeout = cf_transaction_timeout;

			if (user_idle_transaction_timeout > 0)
				effective_idle_transaction_timeout = user_idle_transaction_timeout;

			if (user_query_timeout > 0)
				effective_query_timeout = user_query_timeout;

			if (user_transaction_timeout > 0)
				effective_transaction_timeout = user_transaction_timeout;

			if (effective_query_timeout > 0 && age_client > effective_query_timeout) {
				disconnect_server(server, true, thread_id, "query timeout");
			} else if (effective_idle_transaction_timeout > 0 &&
				   server->idle_tx &&
				   age_server > effective_idle_transaction_timeout) {
				disconnect_server(server, true, thread_id, "idle transaction timeout");
			} else if (effective_transaction_timeout > 0 &&
				   age_transaction > effective_transaction_timeout) {
				disconnect_server(server, true, thread_id, "transaction timeout");
			}
		}
	}

	/* find connections that got connect, but could not log in */
	if (cf_server_connect_timeout > 0) {
		statlist_for_each_safe(item, &pool->new_server_list, tmp) {
			usec_t age;

			server = container_of(item, PgSocket, head);
			Assert(server->state == SV_LOGIN);

			age = now - server->connect_time;
			if (age > cf_server_connect_timeout)
				disconnect_server(server, true, thread_id, "connect timeout");
		}
	}

	check_pool_size(pool, thread_id);
}

/* maintain servers in a peer pool */
static void peer_pool_server_maint(PgPool *pool, int thread_id)
{
	struct List *item, *tmp;
	usec_t now = get_multithread_time_with_id(thread_id);
	PgSocket *server;

	/*
	 * find connections that got connected, but could not log in. For normal
	 * pools we only compare against server_connect_timeout for these servers,
	 * but since peer pools are only for sending cancellations we also compare
	 * against cancel_wait_timeout here.
	 */
	if (cf_server_connect_timeout > 0 || cf_cancel_wait_timeout > 0) {
		statlist_for_each_safe(item, &pool->new_server_list, tmp) {
			usec_t age;

			server = container_of(item, PgSocket, head);
			Assert(server->state == SV_LOGIN);

			age = now - server->connect_time;
			if (cf_server_connect_timeout > 0 && age > cf_server_connect_timeout) {
				disconnect_server(server, true, thread_id, "connect timeout");
			} else if (cf_cancel_wait_timeout > 0 && age > cf_cancel_wait_timeout) {
				disconnect_server(server, true, thread_id, "cancel_wait_timeout");
			}
		}
	}
}


static void cleanup_client_logins(int thread_id)
{
	struct List *item, *tmp;
	PgSocket *client;
	usec_t age;
	struct StatList* login_client_list_ptr;
	usec_t now = get_multithread_time_with_id(thread_id);

	if (cf_client_login_timeout <= 0)
		return;
	login_client_list_ptr = &login_client_list;
	if(multithread_mode){
		login_client_list_ptr = &(threads[thread_id].login_client_list);
	}
	statlist_for_each_safe(item, login_client_list_ptr, tmp) {
		client = container_of(item, PgSocket, head);
		age = now - client->connect_time;
		if (age > cf_client_login_timeout)
			disconnect_client(client, true, thread_id, "client_login_timeout");
	}
}

static void cleanup_inactive_autodatabases_internal(PgDatabase *db, usec_t now, int thread_id){
	usec_t age;
	if (db->db_paused)
		return;
	age = now - db->inactive_time;
	if (age > cf_autodb_idle_timeout) {
		kill_database(db, thread_id);
	}
}

static void cleanup_inactive_autodatabases(int thread_id)
{
	PgDatabase *db;
	struct List *item, *tmp;
	usec_t now = get_multithread_time_with_id(thread_id);
	struct StatList* autodatabase_idle_list_ptr_;

	if (cf_autodb_idle_timeout <= 0)
		return;
	
	autodatabase_idle_list_ptr_ = GET_MULTITHREAD_LIST_PTR(autodatabase_idle_list, thread_id);

	/* now kill the old ones */
	statlist_for_each_safe(item, (struct StatList*)autodatabase_idle_list_ptr_, tmp) {
		db = container_of(item, PgDatabase, head);
		cleanup_inactive_autodatabases_internal(db, now, thread_id);
	}
	
}

static void create_pool_for_forced_user_cb(struct List *item, void *ctx){
	PgDatabase *db = container_of(item, PgDatabase, head);
	int thread_id = *(int*)ctx;
	if (database_min_pool_size(db) > 0 && db->forced_user_credentials != NULL) {
		get_pool(db, db->forced_user_credentials, thread_id);
	}
}

static void clean_pool_internal(PgPool *pool, void* ctx){
	struct {
		int seq;
		int thread_id;
	} *data;
	data = ctx;
	if (pool->db->admin)
		return;
	pool_server_maint(pool, data->thread_id);
	pool_client_maint(pool, data->thread_id);

	/* is autodb active? */
	if (pool->db->db_auto && pool->db->inactive_time == 0) {
		if (pool_client_count(pool) > 0 || pool_server_count(pool) > 0)
			pool->db->active_stamp = data->seq;
	}
}

static void clean_pool_cb(struct List *item, void *ctx) {
	PgPool *pool = container_of(item, PgPool, head);
	clean_pool_internal(pool, ctx);
}

static void move_inactive_to_idle_list_cb(struct List *item, void *ctx){
	PgDatabase *db = container_of(item, PgDatabase, head);
	struct {
		unsigned int seq;
		struct ThreadSafeStatList* database_list_ptr;
		struct StatList* autodatabase_idle_list_ptr;
		int thread_id;
	} *data = ctx;
	if (db->db_auto && db->inactive_time == 0) {
		if (db->active_stamp == data->seq)
			return;
		db->inactive_time = get_multithread_time_with_id(data->thread_id);
		thread_safe_statlist_remove(data->database_list_ptr, &db->head);
		statlist_append(data->autodatabase_idle_list_ptr, &db->head);
	}
}

/* full-scale maintenance, done only occasionally */
static void do_full_maint(evutil_socket_t sock, short flags, void *arg)
{
	struct List *item, *tmp;
	PgPool *pool;
	PgDatabase *db;
	struct StatList* peer_pool_list_ptr;
	void* database_list_ptr;
	void* pool_list_ptr;
	struct StatList* autodatabase_idle_list_ptr;
	struct {
		int seq;
		int thread_id;
	} data;
	int thread_id = get_current_thread_id(multithread_mode);

	static unsigned int seq;
	unsigned int* seq_ptr = &seq;

	if(multithread_mode){
		seq_ptr = &(threads[thread_id].seq);
	}
	(*seq_ptr) ++;

	data.seq = *seq_ptr;
	data.thread_id = thread_id;
	/*
	 * Avoid doing anything that may surprise other pgbouncer.
	 */
	if (cf_pause_mode == P_SUSPEND)
		return;

	/*
	 * Creating new pools to enable `min_pool_size` enforcement even if
	 * there are no active clients.
	 *
	 * If clients never connect there won't be a pool to maintain the
	 * min_pool_size on, which means we have to proactively create a pool,
	 * so that it can be maintained.
	 *
	 * We are doing this for databases with forced users only, to reduce
	 * the risk of creating connections in unexpected ways, where there are
	 * many users.
	   _	 */
	
	peer_pool_list_ptr = (struct StatList*)GET_MULTITHREAD_LIST_PTR(peer_pool_list, thread_id);
	database_list_ptr = GET_MULTITHREAD_LIST_PTR(database_list, thread_id);
	pool_list_ptr = GET_MULTITHREAD_LIST_PTR(pool_list, thread_id);
	autodatabase_idle_list_ptr = (struct StatList*)GET_MULTITHREAD_LIST_PTR(autodatabase_idle_list, thread_id);

	/*
	 * Creating new pools to enable `min_pool_size` enforcement even if
	 * there are no active clients.
	 *
	 * If clients never connect there won't be a pool to maintain the
	 * min_pool_size on, which means we have to proactively create a pool,
	 * so that it can be maintained.
	 *
	 * We are doing this for databases with forced users only, to reduce
	 * the risk of creating connections in unexpected ways, where there are
	 * many users.
	   _	 */
	if(multithread_mode){
		thread_safe_statlist_iterate((struct ThreadSafeStatList*)database_list_ptr, create_pool_for_forced_user_cb, &thread_id);
	} else {
		statlist_for_each_safe(item, (struct StatList*)database_list_ptr, tmp) {
			db = container_of(item, PgDatabase, head);
			if (database_min_pool_size(db) > 0 && db->forced_user_credentials != NULL) {
				get_pool(db, db->forced_user_credentials, -1);
			}
		}
	}

	if (multithread_mode){
		thread_safe_statlist_iterate((struct ThreadSafeStatList*)pool_list_ptr, clean_pool_cb, &data);
	} else {
		statlist_for_each_safe(item, (struct StatList*)pool_list_ptr, tmp) {
			pool = container_of(item, PgPool, head);
			clean_pool_internal(pool, &data);
		}
	}

	statlist_for_each_safe(item, peer_pool_list_ptr, tmp) {
		pool = container_of(item, PgPool, head);
		peer_pool_server_maint(pool, thread_id);
		peer_pool_client_maint(pool, thread_id);
	}

	/* find inactive autodbs */
	if(multithread_mode){
		struct {
			unsigned int seq;
			struct ThreadSafeStatList* database_list_ptr;
			struct StatList* autodatabase_idle_list_ptr;
			int thread_id;
		} data = { *seq_ptr, database_list_ptr, autodatabase_idle_list_ptr, thread_id};
		thread_safe_statlist_iterate((struct ThreadSafeStatList*)database_list_ptr, move_inactive_to_idle_list_cb, &data);
	} else {
		statlist_for_each_safe(item, (struct StatList*)database_list_ptr, tmp) {
			db = container_of(item, PgDatabase, head);
			if (db->db_auto && db->inactive_time == 0) {
				if (db->active_stamp == *seq_ptr)
					continue;
				db->inactive_time = get_cached_time();
				statlist_remove((struct StatList*)database_list_ptr, &db->head);
				statlist_append(autodatabase_idle_list_ptr, &db->head);
			}
		}
	}

	cleanup_inactive_autodatabases(thread_id);

	cleanup_client_logins(thread_id);

	if(multithread_mode){
		Thread* this_thread = (Thread*) pthread_getspecific(thread_pointer);
		if (this_thread->cf_shutdown == SHUTDOWN_WAIT_FOR_SERVERS && get_active_server_count(thread_id) == 0) {
			struct event_base * base;
			log_info("[Thread %d] server connections dropped, exiting", thread_id);
			this_thread->cf_shutdown = SHUTDOWN_IMMEDIATE;
			base = (struct event_base *)pthread_getspecific(event_base_key);
			event_base_loopbreak(base);
			return;
		}

		if (this_thread->cf_shutdown == SHUTDOWN_WAIT_FOR_CLIENTS && get_active_client_count(thread_id) == 0) {
			struct event_base * base;
			log_info("[Thread %d] client connections dropped, exiting", thread_id);
			this_thread->cf_shutdown = SHUTDOWN_IMMEDIATE;
			base = (struct event_base *)pthread_getspecific(event_base_key);
			event_base_loopbreak(base);
			return;
		}
	}else{
		if (cf_shutdown == SHUTDOWN_WAIT_FOR_SERVERS && get_active_server_count(-1) == 0) {
			log_info("server connections dropped, exiting");
			cf_shutdown = SHUTDOWN_IMMEDIATE;
			cleanup_unix_sockets();
			event_base_loopbreak(pgb_event_base);
			return;
		}

		if (cf_shutdown == SHUTDOWN_WAIT_FOR_CLIENTS && get_active_client_count(-1) == 0) {
			log_info("client connections dropped, exiting");
			cf_shutdown = SHUTDOWN_IMMEDIATE;
			cleanup_unix_sockets();
			event_base_loopbreak(pgb_event_base);
			return;
		}
	}

	if(!multithread_mode)
		adns_zone_cache_maint(adns);
}

static void multithread_main_thread_full_maint(evutil_socket_t sock, short flags, void *arg){
	MULTITHREAD_VISIT(multithread_mode, &adns_lock, adns_zone_cache_maint(adns));
	
	FOR_EACH_THREAD(thread_id){
		if(threads[thread_id].cf_shutdown != SHUTDOWN_IMMEDIATE){
			return;
		}
	}
	cf_shutdown = SHUTDOWN_IMMEDIATE;
	cleanup_unix_sockets();
	event_base_loopbreak(pgb_event_base);
}

void main_thread_janitor_setup(void){
	event_assign(&full_maint_ev, pgb_event_base, -1, EV_PERSIST, multithread_main_thread_full_maint, NULL);
			if (event_add(&full_maint_ev, &full_maint_period) < 0)
				log_warning("event_add failed: %s", strerror(errno));
}

/* first-time initialization */
void janitor_setup(void)
{
	/* launch maintenance */
	if(multithread_mode){
		struct event_base * base = (struct event_base *)pthread_getspecific(event_base_key);
		int thread_id = get_current_thread_id(multithread_mode);
		struct event* full_maint_ev_ptr = &(threads[thread_id].full_maint_ev);

		event_assign(full_maint_ev_ptr, base, -1, EV_PERSIST, do_full_maint, NULL);
		if (event_add(full_maint_ev_ptr, &full_maint_period) < 0)
			log_warning("event_add failed: %s", strerror(errno));
	}else{
		event_assign(&full_maint_ev, pgb_event_base, -1, EV_PERSIST, do_full_maint, NULL);
		if (event_add(&full_maint_ev, &full_maint_period) < 0)
			log_warning("event_add failed: %s", strerror(errno));
	}

}

void kill_pool(PgPool *pool, int thread_id)
{
	const char *reason = "database removed";

	close_client_list(&pool->active_client_list, reason, thread_id);
	close_client_list(&pool->waiting_client_list, reason, thread_id);

	close_client_list(&pool->active_cancel_req_list, reason, thread_id);
	close_client_list(&pool->waiting_cancel_req_list, reason, thread_id);

	close_server_list(&pool->active_server_list, reason, thread_id);
	close_server_list(&pool->active_cancel_server_list, reason, thread_id);
	close_server_list(&pool->being_canceled_server_list, reason, thread_id);
	close_server_list(&pool->idle_server_list, reason, thread_id);
	close_server_list(&pool->used_server_list, reason, thread_id);
	close_server_list(&pool->tested_server_list, reason, thread_id);
	close_server_list(&pool->new_server_list, reason, thread_id);

	pktbuf_free(pool->welcome_msg);

	list_del(&pool->map_head);
	if(multithread_mode){
		// TODO check locked
		thread_safe_statlist_remove(&(threads[thread_id].pool_list), &pool->head);
		varcache_clean(&pool->orig_vars);
		slab_free(threads[thread_id].var_list_cache, pool->orig_vars.var_list);
		slab_free(threads[thread_id].pool_cache, pool);
	}else{
		statlist_remove(&pool_list, &pool->head);
		varcache_clean(&pool->orig_vars);
		slab_free(var_list_cache, pool->orig_vars.var_list);
		slab_free(pool_cache, pool);
	}
}

void kill_peer_pool(PgPool *pool, int thread_id)
{
	const char *reason = "peer removed";

	close_client_list(&pool->active_cancel_req_list, reason, thread_id);
	close_client_list(&pool->waiting_cancel_req_list, reason, thread_id);
	close_server_list(&pool->active_cancel_server_list, reason, thread_id);
	close_server_list(&pool->new_server_list, reason, thread_id);

	pktbuf_free(pool->welcome_msg);

	list_del(&pool->map_head);
	if(thread_id != -1){
		statlist_remove(&(threads[thread_id].peer_pool_list), &pool->head);
		varcache_clean(&pool->orig_vars);
		slab_free(threads[thread_id].var_list_cache, pool->orig_vars.var_list);
		slab_free(threads[thread_id].pool_cache, pool);
	}else{
		statlist_remove(&peer_pool_list, &pool->head);
		varcache_clean(&pool->orig_vars);
		slab_free(var_list_cache, pool->orig_vars.var_list);
		slab_free(pool_cache, pool);
	}
}

static void kill_database_cb(struct List *item, void *ctx){
	struct {
		PgDatabase *db;
		int thread_id;
	} *data = ctx;
	PgPool *pool = container_of(item, PgPool, head);
	if (pool->db->name == data->db->name)
		kill_pool(pool, data->thread_id);
}

void kill_database(PgDatabase *db, int thread_id)
{
	PgPool *pool;
	struct List *item, *tmp;

	void* autodatabase_idle_list_ptr = GET_MULTITHREAD_LIST_PTR(autodatabase_idle_list, thread_id);
	void* database_list_ptr = GET_MULTITHREAD_LIST_PTR(database_list, thread_id);
	struct Slab* db_cache_ptr = db_cache;
	if(thread_id != -1){
		db_cache_ptr = threads[thread_id].db_cache;
	}

	log_warning("dropping database '%s' as it does not exist anymore or inactive auto-database", db->name);

	if(multithread_mode){
		struct {
			PgDatabase *db;
			int thread_id;
		} data = {db, thread_id};
		thread_safe_statlist_iterate(&(threads[thread_id].pool_list), kill_database_cb, &data);
	} else {
		statlist_for_each_safe(item, &pool_list, tmp) {
			pool = container_of(item, PgPool, head);
			if (pool->db->name == db->name)
				kill_pool(pool, -1);
		}
	}

	pktbuf_free(db->startup_params);
	free(db->host);
	
	if (db->forced_user_credentials) {
		if(multithread_mode){
			thread_safe_slab_free(thread_safe_credentials_cache, db->forced_user_credentials);
		} else {
			slab_free(credentials_cache, db->forced_user_credentials);
		}
	}
	free(db->connect_query);
	if (db->inactive_time) {
		if(multithread_mode){
			thread_safe_statlist_remove((struct ThreadSafeStatList *)autodatabase_idle_list_ptr, &db->head);
		} else {
			statlist_remove((struct StatList *)autodatabase_idle_list_ptr, &db->head);
		}
	} else {
		if(multithread_mode){
			thread_safe_statlist_remove((struct ThreadSafeStatList *)database_list_ptr, &db->head);
		} else {
			statlist_remove((struct StatList *)database_list_ptr, &db->head);
		}
	}

	if (db->auth_dbname)
		free((void *)db->auth_dbname);

	if (db->auth_query)
		free((void *)db->auth_query);

	/* Cleanup cached scram keys stored with PgCredentials */
	clear_user_tree_cached_scram_keys(&db->user_tree);
	aatree_destroy(&db->user_tree);

	slab_free(db_cache_ptr, db);
}

void kill_peer(PgDatabase *db, int thread_id)
{
	PgPool *pool;
	struct List *item, *tmp;

	log_warning("dropping peer %s as it does not exist anymore", db->name);

	if(thread_id != -1){
		statlist_for_each_safe(item, &(threads[thread_id].peer_pool_list), tmp) {
			pool = container_of(item, PgPool, head);
			if (pool->db->name == db->name)
				kill_peer_pool(pool, thread_id);
		}
	}else{
		statlist_for_each_safe(item, &peer_pool_list, tmp) {
			pool = container_of(item, PgPool, head);
			if (pool->db->name == db->name)
				kill_peer_pool(pool, -1);
		}
	}
	free(db->host);

	statlist_remove(&peer_list, &db->head);
	slab_free(peer_cache, db);
}

/* as [pgbouncer] section can be loaded after databases,
   there's need for review */
void config_postprocess(void)
{
	struct List *item, *tmp;
	PgDatabase *db;
	
	if(multithread_mode){
		// TODO fix this!!
		FOR_EACH_THREAD(thread_id){
			// lock_and_pause_thread(thread_id);
			statlist_for_each_safe(item, &database_list, tmp) {
				db = container_of(item, PgDatabase, head);
				if (db->db_dead) {
					kill_database(db, thread_id);
					continue;
				}
			}

			statlist_for_each_safe(item, &peer_list, tmp) {
				db = container_of(item, PgDatabase, head);
				if (db->db_dead) {
					kill_peer(db, thread_id);
					continue;
				}
			}
			// unlock_and_resume_thread(thread_id);
		}
	}else{
		statlist_for_each_safe(item, &database_list, tmp) {
			db = container_of(item, PgDatabase, head);
			if (db->db_dead) {
				kill_database(db, -1);
				continue;
			}
		}

		statlist_for_each_safe(item, &peer_list, tmp) {
			db = container_of(item, PgDatabase, head);
			if (db->db_dead) {
				kill_peer(db, -1);
				continue;
			}
		}
	}
}

static void clean_cached_scram(struct AANode *n, void *arg)
{
	struct PgCredentials *user = container_of(n, struct PgCredentials, tree_node);
	if (user->scram_SaltKey != NULL) {
		free(user->scram_SaltKey);
		user->scram_SaltKey = NULL;
		user->adhoc_scram_secrets_cached = false;
	}
}

void clear_user_tree_cached_scram_keys(struct AATree *tree)
{
	aatree_walk(tree, AA_WALK_IN_ORDER, clean_cached_scram, NULL);
}
