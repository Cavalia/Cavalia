#pragma once
#ifndef __COMMON_NUMA_HELPER_H__
#define __COMMON_NUMA_HELPER_H__

#include <iostream>
#include <vector>
#if defined(NUMA)
#include <numa.h>
#endif

#if defined(NUMA)
struct NumaTopology{
	NumaTopology(){
		// number of cores in the machine.
		max_core_count_ = numa_num_task_cpus();
		// number of numa nodes in the machine.
		max_node_count_ = numa_num_task_nodes();
		// number of cores in each numa node.
		core_per_node_ = max_core_count_ / max_node_count_;

		cores_.resize(max_node_count_);
		//bitmask *bm = numa_bitmask_alloc(max_core_count_);
		bitmask *bm = numa_allocate_cpumask();
		for (size_t i = 0; i < max_node_count_; ++i) {
			numa_node_to_cpus(i, bm);
			for (size_t j = 0; j < max_core_count_; ++j) {
				if (numa_bitmask_isbitset(bm, j)) {
					cores_[i].push_back(j);
				}
			}
		}
		numa_free_nodemask(bm);
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
#endif

static size_t GetNumaNodeId(const size_t &core_id){
#if defined(NUMA)
	return numa_node_of_cpu(core_id);
#else
	return 0;
#endif
}

static size_t GetCoreInNode(const size_t &numa_node_id){
#if defined(NUMA)
	size_t max_core_count = numa_num_task_cpus();
	bitmask *bm = numa_allocate_cpumask();
	numa_node_to_cpus(numa_node_id, bm);
	for (size_t i = 0; i < max_core_count; ++i){
		if (numa_bitmask_isbitset(bm, i)){
			numa_free_nodemask(bm);
			return i;
		}
	}
	assert(false);
	return -1;
#else
	return 0;
#endif
}

#endif
