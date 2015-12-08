#pragma once
#ifndef __CAVALIA_BENCHMARK_FRAMEWORK_BENCHMARK_HSTORE_CONFIGURATION_H__
#define __CAVALIA_BENCHMARK_FRAMEWORK_BENCHMARK_HSTORE_CONFIGURATION_H__

#include <cassert>
#include <NumaHelper.h>
#include "../Storage/ShardTableLocation.h"
#include "../Executor/HStoreTxnLocation.h"

namespace Cavalia {
	namespace Benchmark {
		using namespace Cavalia::Database;

		class BenchmarkHStoreConfiguration {
		public:
			BenchmarkHStoreConfiguration(const size_t &core_count, const size_t &node_count) : core_count_(core_count), node_count_(node_count) {}
			virtual ~BenchmarkHStoreConfiguration() {}

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
					for (size_t i = 0; i < node_count_; ++i){
						txn_location_.AddThread(occupied_cores.at(i).at(k));
						table_location_.AddPartition(i);
					}
				}
			}

			const HStoreTxnLocation& GetHStoreTxnLocation() const {
				return txn_location_;
			}

			const ShardTableLocation& GetTableLocation() const {
				return table_location_;
			}

		private:
			virtual size_t GetTableCount() const = 0;

		private:
			BenchmarkHStoreConfiguration(const BenchmarkHStoreConfiguration&);
			BenchmarkHStoreConfiguration& operator=(const BenchmarkHStoreConfiguration&);

		private:
			const size_t core_count_;
			const size_t node_count_;
			// <thread_id, core_id>
			HStoreTxnLocation txn_location_;
			// <partition_id, numa_node_id>
			ShardTableLocation table_location_;
		};
	}
}

#endif
