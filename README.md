# Cavalia
A transactional main-memory database on multicores.

## Features

Cavalia is a main-memory database system specifically designed for modern multicore architectures. It aims at providing a generalized framework for comparing different concurrency-control mechanisms. A highlight in Cavalia is that new hardware features (e.g., NUMA, HTM, and NVRAM) are judiciously leveraged for achieving higher level of concurrency when processing massive volume of transactions.

## Disclaimers
Our project aims at faithfully implementing all kinds of concurrency-control and failure-recovery schemes in the same database framework. Currently, this project is still under instensive development. Please feel free to contact us if you find any bugs or errors in our implementation. Thanks!

## Linux Platform Installation

1. Download and install dependent libraries, including: boost-1.55.0 and libcuckoo;
2. Clone Cavalia project, update CMakeLists.txt to set dependent library directories. 
3. Build the project using the following command: mkdir build; cd build; cmake -DCMAKE_INSTALL_PREFIX=/to/your/directory ..; make -j; make install.
* Please note that the project requires g++ 4.8 with C++11 enabled.


## Compile Options
### Concurrency control
* SILOCK: locking-based snapshot isolation with no-wait strategy [BBG+95].
* SIOCC: optimistic snapshot isolation [BBG+95].
* MVTO: multi-version timestamp ordering [BHG87, CM86, YBP+14].
* MVLOCK_WAIT: multi-version two-phase locking with wait-die strategy [CM86].
* MVLOCK: multi-version two-phase locking with no-wait strategy [CM86].
* TVLOCK: two-version two-phase locking with no-wait strategy [CM86].
* MVOCC: multi-version optimistic concurrency control [CM86].
* TO: timestamp ordering [BHG87, YBP+14].
* LOCK_WAIT: two-phase locking with wait-die strategy [BHG87, YBP+14].
* LOCK: two-phase locking with no-wait strategy [BHG87, YBP+14].
* OCC: optimistic concurrency control [BHG87, YBP+14].
* SILO: an implementation following silo's design [TZK+13].
* DBX: an implementation following DBX's design [WQLC14].
* ST: disable concurrency control. must be turned on when performing log replay [MWMS14, ZTKL14].

### Index
* CUCKOO_INDEX: enable cuckoo index (See https://github.com/efficient/libcuckoo).

### Logging
* VALUE_LOGGING: enable value logging [ZTKL14].
* COMMAND_LOGGING: enable command logging [MWMS14].
* COMPRESSION: enable log compression.

### Timestamp allocation
* BATCH_TIMESTAMP: allocate timestamp in batch.
* SCALABLE_TIMESTAMP: allocate timestamp in Silo's style [TZK+13,].

### Profiler
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

### Hardware architecture
* PTHREAD_LOCK: use pthread_spin_lock.
* BUILTIN_LOCK: use __sync_lock_test_and_set.

## Notes
* please turn off all the cc-related options when testing transaction replays.
* the memory allocated for storage manager, including indexes and records, goes unmanaged -- we do not reclaim them throughout the lifetime.

## References
* [BBG+95] Hal Berenson, Phil Bernstein, Jim Gray, Jim Melton, Elizabeth O’Neil, and Patrick O’Neil. A Critique of ANSI SQL Isolation Levels. In SIGMOD, 1995.
* [BHG87] Philip A Bernstein, Vassos Hadzilacos, and Nathan Goodman. Concurrency Control and Recovery in Database Systems. 1987.
* [CM86] Michael J Carey and Waleed A Muhanna. The Performance of Multiversion Concurrency Control
Algorithms. TOCS, 1986.
* [MWMS14] Nirmesh Malviya, Ariel Weisberg, Samuel Madden,
and Michael Stonebraker. Rethinking Main Memory
OLTP Recovery. In ICDE, 2014.
* [TZK+13] Stephen Tu, Wenting Zheng, Eddie Kohler, Barbara
Liskov, and Samuel Madden. Speedy Transactions in
Multicore In-memory Databases. In SOSP, 2013.
* [WQLC14] Zhaoguo Wang, Hao Qian, Jinyang Li, and Haibo
Chen. Using Restricted Transactional Memory to Build
a Scalable In-Memory Database. In EuroSys, 2014.
* [YBP+14] Xiangyao Yu, George Bezerra, Andrew Pavlo,
Srinivas Devadas, and Michael Stonebraker. Staring
into the Abyss: An Evaluation of Concurrency Control
with One Thousand Cores. In VLDB, 2014.
* [ZTKL14] Wenting Zheng, Stephen Tu, Eddie Kohler, and
Barbara Liskov. Fast Databases with Fast Durability
and Recovery Through Multicore Parallelism. In
OSDI, 2014.


## Licence
Copyright (C) 2015-16, School of Computing, National University of Singapore

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

