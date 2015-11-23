#Cavalia
A transactional multicore main-memory database.

##Linux Platform Installation

1. Download and install dependent libraries, including: boost-1.55.0, gtest-1.7.0, protobuf-2.5.0, libcuckoo;
2. Clone Cavalia project, update CMakeLists.txt to set dependent library directories. 
3. Build the project using the following command: mkdir build; cd build; cmake -DCMAKE_INSTALL_PREFIX=/to/your/directory ..; make -j; make install.

Please note that the project requires g++ 4.8 with C++11 enabled.

##Compile Options
###Concurrency control
* ST: disable cc.
* SILOCK (not ready): locking-based snapshot isolation with no-wait strategy.
* SIOCC (not ready): optimistic snapshot isolation.
* MVTO: multi-version timestamp ordering.
* MVLOCK: multi-version two-phase locking with no-wait strategy.
* MVLOCK_WAIT: multi-version two-phase locking with wait-die strategy.
* MVOCC: multi-version optimistic concurrency control.
* TO: timestamp ordering.
* LOCK: two-phase locking with no-wait strategy.
* LOCK_WAIT: two-phase locking with wait-die strategy.
* OCC: optimistic concurrency control.
* SILO: an implementation following silo's design.
* HYBRID: a combination of 2PL and OCC.
* NOVALID: mute validation (requires OCC or SILO enabled).

###Timestamp allocation
* BATCH_TIMESTAMP: allocate timestamp in batch.

###Profiler
* MUTE: mute profiling.
* PRECISE_TIMER: use rdtsc timer.
* PROFILE_PHASE: profile execution time of each transaction phase (insert, select, and commit).
* PROFILE_EXECUTION: profile execution time of each stored procedure.
* PROFILE_CC_WAIT: measure wait time on each table.
* PROFILE_CC_ABORT_COUNT: count number of aborts on each table.
* PROFILE_CC_ABORT_TIME: measure abort time.
* PROFILE_CC_TS_ALLOC: measure timestamp allocation time.
* PROFILE_CC_MEM_ALLOC: measure memory allocation time.
* PROFILE_CC_EXECUTION_TIME: measure time breakdown of the current concurrency control algorithm.
* PROFILE_CC_EXECUTION_COUNT: measure statistics of the current concurrency control algorithm.

###Hardware architecture
* NUMA: enable NUMA awareness.
* PTHREAD_LOCK: use pthread_spin_lock.
* BUILTIN_LOCK: use __sync_lock_test_and_set.

###Index
* CUCKOO_INDEX: enable cuckoo index.

###Logging
* VALUE_LOGGING: enable value logging.
* COMMAND_LOGGING: enable command logging.

##Notes
* please turn off all the cc-related options when testing transaction replays.
* the memory allocated for storage manager, including indexes and records, goes unmanaged -- we do not reclaim them throughout the lifetime.

##Authors
* Yingjun Wu \<yingjun AT comp.nus.edu.sg\>
* Wentian Guo \<wentian AT comp.nus.edu.sg\>

Please feel free to contact us if you confront any problem when using Cavalia. Thanks!
