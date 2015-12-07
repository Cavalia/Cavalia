#pragma once
#ifndef __CAVALIA_BENCHMARK_FRAMEWORK_BENCHMARK_SITE_CONFIGURATION_H__
#define __CAVALIA_BENCHMARK_FRAMEWORK_BENCHMARK_SITE_CONFIGURATION_H__

#include "../Meta/MetaTypes.h"
#include "NumaTopology.h"

namespace Cavalia {
	namespace Benchmark {
		using namespace Cavalia::Database;
		class BenchmarkSiteConfiguration {
		public:
			BenchmarkSiteConfiguration(const size_t &core_count, const size_t &node_count) : core_count_(core_count), node_count_(node_count) {}
			~BenchmarkSiteConfiguration() {}

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

				size_t table_count = GetTableCount();
				table_locations_.resize(table_count);
				for (size_t tab_id = 0; tab_id < table_count; ++tab_id){
					for (size_t node_id = 0; node_id < node_count_; ++node_id){
						table_locations_[tab_id].node_ids_.push_back(node_id);
					}
				}
				for(size_t j = 0; j < core_count_; ++j){
					for (size_t i = 0; i < node_count_; ++i){
						txn_location_.core_ids_.push_back(occupied_cores.at(i).at(j));
					}
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
			BenchmarkSiteConfiguration(const BenchmarkSiteConfiguration&);
			BenchmarkSiteConfiguration& operator=(const BenchmarkSiteConfiguration&);

		private:
			const size_t core_count_;
			const size_t node_count_;
			// <partition_id, core_id>
			// partition_id is essentially the same as thread_id.
			TxnLocation  txn_location_;
			// table_id => <partition_id, numa_node_id>
			std::vector<TableLocation> table_locations_;
		};
	}
}

#endif
