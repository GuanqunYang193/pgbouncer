## Design Principles of Multithreaded PgBouncer

* **Thread-Isolated Execution:** PgBouncer employs a per-thread architecture where each thread runs independently with its own event loop, client/server connections, and internal state. This avoids global synchronization and enables high concurrency with minimal contention.

* **Thread-Local Memory Caching:** Each thread maintains its own memory pools for connection-related objects, buffers, and internal state. This reduces allocator contention and improves reuse by eliminating the need for cross-thread coordination.

* **Independent Event Loops:** Every thread initializes a dedicated `libevent` loop (`event_base`) to manage I/O operations. Event bases are stored using `pthread_setspecific()` to ensure thread-local access without shared state.

* **Graceful Thread Coordination:** PgBouncer introduces a lightweight mechanism for pausing and resuming threads to support graceful shutdown and maintenance operations. This avoids system-wide locks while allowing safe coordination when needed.


## Merged PR

The following pull requests involving the new multithreaded data structures and lock mechanisms have already been merged:

* **[Pthread Thread Local Storage Support](https://github.com/libusual/libusual/pull/64)**:
  Introduced support for pthread thread-local storage (TLS), enabling each thread to maintain its own instance of specific data. This enhancement prevents data races and ensures thread safety by isolating thread-specific data.

* **[Fix Unit Tests and Continuous Integration](https://github.com/libusual/libusual/pull/65)**:
  Addressed issues in unit tests and continuous integration (CI) configurations to ensure reliable testing outcomes and maintain code quality across different environments.

* **[Add Spinlock Implementation](https://github.com/libusual/libusual/pull/66)**:
  Implemented a spinlock mechanism utilizing atomic compare-and-swap (CAS) operations. This lock-free synchronization primitive is designed for scenarios with short lock durations, reducing context-switch overhead and enhancing performance in multi-threaded contexts.

* **[Thread-Safe Implementation of StatList](https://github.com/libusual/libusual/pull/67)**:
  Enhanced the `StatList` data structure to be thread-safe by integrating atomic CAS locking mechanisms. This ensures consistent behavior when accessed concurrently by multiple threads.

* **[Thread-Safe Implementation of Slab](https://github.com/libusual/libusual/pull/68)**:
  Modified the `Slab` allocator to support thread-safe operations using spinlocks.

* **[Add Context Support to Thread-Safe StatList Iterators](https://github.com/libusual/libusual/pull/71)**:
  Extended the thread-safe `StatList` iterators to include context support, allowing for more flexible and safe iteration patterns in concurrent environments. 
