#pragma once
#ifndef __CAVALIA_BENCHMARK_FRAMEWORK_BENCHMARK_ISLAND_CONFIGURATION_H__
#define __CAVALIA_BENCHMARK_FRAMEWORK_BENCHMARK_ISLAND_CONFIGURATION_H__

#include "../Meta/MetaTypes.h"
#include "NumaTopology.h"

namespace Cavalia {
	namespace Benchmark {
		using namespace Cavalia::Database;
		class BenchmarkIslandConfiguration {
		public:
			BenchmarkIslandConfiguration(const size_t &core_count, const size_t &node_id) : core_count_(core_count), node_id_(node_id) {}
			~BenchmarkIslandConfiguration() {}

			void MeasureConfiguration() {
				NumaTopology topology;
				topology.Print();
				assert(node_id_ < topology.max_node_count_);
				assert(core_count_ <= topology.core_per_node_);

				std::vector<size_t> occupied_cores;
				std::cout << "cores in node " << node_id_ << ":";
				for (size_t k = 0; k < core_count_; ++k) {
					std::cout << " " << topology.cores_.at(node_id_).at(k);
					occupied_cores.push_back(topology.cores_.at(node_id_).at(k));
				}
				std::cout << std::endl;

				table_locations_.resize(GetTableCount());
				for (size_t tab_id = 0; tab_id < GetTableCount(); ++tab_id){
					table_locations_[tab_id].node_ids_.push_back(node_id_);
				}
				for (size_t k = 0; k < core_count_; ++k){
					txn_location_.core_ids_.push_back(occupied_cores.at(k));
				}
				txn_location_.node_count_ = node_count_;
			}

			const TxnLocation& GetTxnLocation() const {
				return txn_location_;
			}

			const std::vector<TableLocation>& GetTableLocations() const {
				return table_locations_;
			}

		private:
			virtual size_t GetTableCount() const = 0;

		private:
			BenchmarkIslandConfiguration(const BenchmarkIslandConfiguration&);
			BenchmarkIslandConfiguration& operator=(const BenchmarkIslandConfiguration&);

		private:
			const size_t core_count_;
			const size_t node_id_;
			// <partition_id, core_id>
			// partition_id is essentially the same as thread_id.
			TxnLocation  txn_location_;
			// table_id => <partition_id, numa_node_id>
			std::vector<TableLocation> table_locations_;
		};
	}
}

#endif
