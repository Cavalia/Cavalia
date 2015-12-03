#Cavalia
A transactional main-memory database on multicores.

##Features

Cavalia is a main-memory database system specifically designed for modern multicore architectures. It aims at providing a generalized framework for comparing different concurrency-control mechanisms. A highlight in Cavalia is that new hardware features (e.g., NUMA, HTM, and NVRAM) are judiciously leveraged for achieving higher level of concurrency when processing massive volume of transactions. Cavalia is an ongoing project that is under intensive development. Please feel free to contact us if you confront any problem when using Cavalia. Thanks!

##Linux Platform Installation

1. Download and install dependent libraries, including: boost-1.55.0, gtest-1.7.0, protobuf-2.5.0, libcuckoo;
2. Clone Cavalia project, update CMakeLists.txt to set dependent library directories. 
3. Build the project using the following command: mkdir build; cd build; cmake -DCMAKE_INSTALL_PREFIX=/to/your/directory ..; make -j; make install.

Please note that the project requires g++ 4.8 with C++11 enabled.

##Compile Options
###Concurrency control
* SILOCK (not ready): locking-based snapshot isolation with no-wait strategy.
* SIOCC (not ready): optimistic snapshot isolation.
* MVTO (not ready): multi-version timestamp ordering.
* MVLOCK_WAIT (not ready): multi-version two-phase locking with wait-die strategy.
* MVLOCK (not ready): multi-version two-phase locking with no-wait strategy.
* MVOCC (not ready): multi-version optimistic concurrency control.
* TO: timestamp ordering.
* LOCK_WAIT: two-phase locking with wait-die strategy.
* LOCK: two-phase locking with no-wait strategy.
* OCC: optimistic concurrency control.
* HYBRID: a combination of 2PL and OCC.
* SILO: an implementation following silo's design. (see http://dl.acm.org/citation.cfm?id=2522713)
* DBX: an implementation following DBX's design. (see http://dl.acm.org/citation.cfm?id=2592815)
* ST: disable cc.

###Index
* CUCKOO_INDEX: enable cuckoo index.

###Logging
* VALUE_LOGGING: enable value logging.
* COMMAND_LOGGING: enable command logging.

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
* PTHREAD_LOCK: use pthread_spin_lock.
* BUILTIN_LOCK: use __sync_lock_test_and_set.

##Notes
* please turn off all the cc-related options when testing transaction replays.
* the memory allocated for storage manager, including indexes and records, goes unmanaged -- we do not reclaim them throughout the lifetime.

##Authors
* Yingjun Wu \<yingjun AT comp.nus.edu.sg\>
* Wentian Guo \<wentian AT comp.nus.edu.sg\>

##Licence
Copyright (C) 2015, School of Computing, National University of Singapore

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

