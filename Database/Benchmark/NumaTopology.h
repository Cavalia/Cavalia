#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_BENCHMARK_NUMA_TOPOLOGY_H__
#define __CAVALIA_TPCC_BENCHMARK_BENCHMARK_NUMA_TOPOLOGY_H__

#include <iostream>
#include <vector>
#include <numa.h>

namespace Cavalia{
	namespace Benchmark{
		struct NumaTopology{
			NumaTopology(){
				// number of cores in the machine.
				max_core_count_ = numa_num_task_cpus();
				// number of numa nodes in the machine.
				max_node_count_ = numa_num_task_nodes();
				// number of cores in each numa node.
				core_per_node_ = max_core_count_ / max_node_count_;

				cores_.resize(max_node_count_);
				
				bitmask *bm = numa_bitmask_alloc(max_core_count_);
				for (size_t i = 0; i < max_node_count_; ++i) {
					numa_node_to_cpus(i, bm);
					for (size_t j = 0; j < max_core_count_; ++j) {
						if (numa_bitmask_isbitset(bm, j)) {
							cores_[i].push_back(j);
						}
					}
				}
				numa_bitmask_free(bm);
			}
			
			void Print(){
				std::cout << "max core count = " << max_core_count_ << std::endl;
				std::cout << "max node count = " << max_node_count_ << std::endl;
				std::cout << "core per node = " << core_per_node_ << std::endl;
			}

			size_t max_core_count_;
			size_t max_node_count_;
			size_t core_per_node_;
			std::vector<std::vector<size_t>> cores_;
		};
	}
}
#endif
