#pragma once
#ifndef __CAVALIA_BENCHMARK_FRAMEWORK_BENCHMARK_CENTRAL_FLOW_CONFIGURATION_H__
#define __CAVALIA_BENCHMARK_FRAMEWORK_BENCHMARK_CENTRAL_FLOW_CONFIGURATION_H__

#include <vector>
#include <MetaTypes.h>

namespace Cavalia {
	namespace Benchmark {
		using namespace Cavalia::StorageEngine;
		class BenchmarkCentralFlowConfiguration {
		public:
			BenchmarkCentralFlowConfiguration(const size_t &core_count, const size_t &node_count) : core_count_(core_count), node_count_(node_count){}
			~BenchmarkCentralFlowConfiguration() {}

			virtual void MeasureConfiguration() = 0;

			const std::vector<size_t>& GetSliceCounts() const {
				return slice_counts_;
			}

			const std::vector<TableLocation>& GetTableLocations() const {
				return table_locations_;
			}

		private:
			BenchmarkCentralFlowConfiguration(const BenchmarkCentralFlowConfiguration&);
			BenchmarkCentralFlowConfiguration& operator=(const BenchmarkCentralFlowConfiguration&);

		protected:
			const size_t core_count_;
			const size_t node_count_;
			std::vector<size_t> slice_counts_;
			std::vector<TableLocation> table_locations_;
		};
	}
}

#endif
