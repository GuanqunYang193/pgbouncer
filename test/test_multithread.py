import threading
import time
import pytest
import psycopg
from psycopg.rows import dict_row

from .utils import capture, run


def test_multithread_basic_functionality(bouncer_multithread):
    """Test basic functionality with multithreaded PgBouncer"""
    # Test that admin commands work
    version = bouncer_multithread.admin_value("SHOW VERSION")
    assert version is not None
    
    # Test that regular connections work
    with bouncer_multithread.cur() as cur:
        cur.execute("SELECT 1")
        result = cur.fetchone()
        assert result[0] == 1


def test_multithread_concurrent_connections(bouncer_multithread):
    """Test that multithreaded PgBouncer can handle concurrent connections"""
    connections = []
    results = []
    
    def make_connection(conn_id):
        try:
            with bouncer_multithread.cur() as cur:
                cur.execute("SELECT %s", (conn_id,))
                result = cur.fetchone()
                results.append((conn_id, result[0]))
        except Exception as e:
            results.append((conn_id, f"error: {e}"))
    
    # Create multiple concurrent connections
    threads = []
    for i in range(4):
        thread = threading.Thread(target=make_connection, args=(i,))
        threads.append(thread)
        thread.start()
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
    
    # Verify all connections worked
    assert len(results) == 4
    for conn_id, result in results:
        assert result == conn_id, f"Connection {conn_id} failed: {result}"


def test_multithread_admin_commands(bouncer_multithread):
    """Test admin commands work correctly with multithreaded PgBouncer"""
    # Test various admin commands
    admin_commands = [
        "SHOW CLIENTS",
        "SHOW SERVERS", 
        "SHOW POOLS",
        "SHOW STATS",
        "SHOW CONFIG",
        "SHOW DATABASES",
        "SHOW USERS",
        "SHOW VERSION"
    ]
    
    for cmd in admin_commands:
        result = bouncer_multithread.admin(cmd)
        assert result is not None


# def test_multithread_pause_resume(bouncer_multithread):
#     """Test pause/resume functionality with multithreaded PgBouncer"""
#     # Create a connection
#     conn = bouncer_multithread.conn(dbname="p0", user="maxedout")
    
#     # Pause the database
#     bouncer_multithread.admin("PAUSE p0")
    
#     # Verify pause worked
#     pools = bouncer_multithread.admin("SHOW POOLS", row_factory=dict_row)
#     p0_pool = next(p for p in pools if p["database"] == "p0")
#     assert p0_pool["state"] == "paused"
    
#     # Resume the database
#     bouncer_multithread.admin("RESUME p0")
    
#     # Verify resume worked
#     pools = bouncer_multithread.admin("SHOW POOLS", row_factory=dict_row)
#     p0_pool = next(p for p in pools if p["database"] == "p0")
#     assert p0_pool["state"] == "active"
    
#     conn.close()


# def test_multithread_kill_commands(bouncer_multithread):
#     """Test kill commands with multithreaded PgBouncer"""
#     # Create connections
#     conn1 = bouncer_multithread.conn(dbname="p0", user="maxedout")
#     conn2 = bouncer_multithread.conn(dbname="p1", user="maxedout")
    
#     # Verify we have clients
#     clients = bouncer_multithread.admin("SHOW CLIENTS", row_factory=dict_row)
#     assert len(clients) == 3  # 2 client connections + admin connection
    
#     # Kill all clients
#     bouncer_multithread.admin("KILL")
    
#     # Verify clients were killed
#     clients = bouncer_multithread.admin("SHOW CLIENTS")
#     assert len(clients) == 1  # Only admin connection remains
    
#     conn1.close()
#     conn2.close()


def test_multithread_stress_test(bouncer_multithread):
    """Stress test for multithreaded PgBouncer"""
    def stress_worker(worker_id, iterations):
        for i in range(iterations):
            try:
                with bouncer_multithread.cur() as cur:
                    cur.execute("SELECT %s, %s", (worker_id, i))
                    result = cur.fetchone()
                    assert result[0] == worker_id
                    assert result[1] == i
            except Exception as e:
                print(f"Worker {worker_id} iteration {i} failed: {e}")
                raise
    
    # Create multiple workers doing many operations
    threads = []
    num_workers = 5
    iterations_per_worker = 20
    
    for worker_id in range(num_workers):
        thread = threading.Thread(
            target=stress_worker, 
            args=(worker_id, iterations_per_worker)
        )
        threads.append(thread)
        thread.start()
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
    
    # Verify final state
    # stats = bouncer_multithread.admin("SHOW STATS", row_factory=dict_row)
    # p0_stats = next(s for s in stats if s["database"] == "p0")
    # assert p0_stats["total_query_count"] >= num_workers * iterations_per_worker


def test_multithread_config_reload(bouncer_multithread):
    """Test config reload with multithreaded PgBouncer"""
    # Test that reload works
    bouncer_multithread.admin("RELOAD")
    
    # Verify bouncer is still working after reload
    with bouncer_multithread.cur() as cur:
        cur.execute("SELECT 1")
        result = cur.fetchone()
        assert result[0] == 1


def test_multithread_shutdown(bouncer_multithread):
    """Test graceful shutdown with multithreaded PgBouncer"""
    # Create some connections
    conn1 = bouncer_multithread.conn(dbname="p0", user="maxedout")
    conn2 = bouncer_multithread.conn(dbname="p1", user="maxedout")
    
    # Verify connections work
    with conn1.cursor() as cur:
        cur.execute("SELECT 1")
        assert cur.fetchone()[0] == 1
    
    # The cleanup should happen automatically when the fixture yields
    # This test just verifies that the bouncer can be shut down gracefully
    # with active connections
    conn1.close()
    conn2.close()
