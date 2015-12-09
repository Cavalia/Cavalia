#pragma once
#ifndef __CAVALIA_BENCHMARK_FRAMEWORK_BENCHMARK_ISLAND_CONFIGURATION_H__
#define __CAVALIA_BENCHMARK_FRAMEWORK_BENCHMARK_ISLAND_CONFIGURATION_H__

#include <cassert>
#include <NumaHelper.h>
#include "../Storage/IslandTableLocation.h"
#include "../Executor/IslandTxnLocation.h"

namespace Cavalia {
	namespace Benchmark {
		using namespace Cavalia::Database;

		class BenchmarkIslandConfiguration {
		public:
			BenchmarkIslandConfiguration(const size_t &core_count, const size_t &node_count, const size_t &node_id) : core_count_(core_count), node_count_(node_count), node_id_(node_id) {}
			virtual ~BenchmarkIslandConfiguration() {}

			void MeasureConfiguration() {
				NumaTopology topology;
				topology.Print();
				assert(node_count_ <= topology.max_node_count_);
				assert(core_count_ <= topology.core_per_node_);

				std::vector<std::vector<size_t>> occupied_cores;
				occupied_cores.resize(node_count_);
				for (size_t node_id = 0; node_id < node_count_; ++node_id) {
					std::cout << "cores in node " << node_id << ":";
					for (size_t k = 0; k < core_count_; ++k) {
						size_t core_id = topology.cores_.at(node_id).at(k);
						std::cout << " " << core_id;
						occupied_cores.at(node_id).push_back(core_id);
					}
					std::cout << std::endl;
				}

				for (size_t k = 0; k < core_count_; ++k){
					txn_location_.AddThread(occupied_cores.at(node_id_).at(k));
				}
				txn_location_.SetPartitionCount(node_count_);

				for (size_t node_id = 0; node_id < node_count_; ++node_id){
					table_location_.AddPartition(node_id);
				}
				table_location_.SetPartitionId(node_id_);
			}

			const IslandTxnLocation& GetIslandTxnLocation() const {
				return txn_location_;
			}

			const IslandTableLocation& GetTableLocation() const {
				return table_location_;
			}

		private:
			virtual size_t GetTableCount() const = 0;

		private:
			BenchmarkIslandConfiguration(const BenchmarkIslandConfiguration&);
			BenchmarkIslandConfiguration& operator=(const BenchmarkIslandConfiguration&);

		private:
			const size_t core_count_;
			const size_t node_count_;
			const size_t node_id_;
			// <thread_id, core_id>
			IslandTxnLocation  txn_location_;
			// <partition_id, numa_node_id>
			IslandTableLocation table_location_;
		};
	}
}

#endif
