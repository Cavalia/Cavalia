#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_TPCC_ISLAND_CONFIGURATION_H__
#define __CAVALIA_TPCC_BENCHMARK_TPCC_ISLAND_CONFIGURATION_H__

#include <Benchmark/BenchmarkIslandConfiguration.h>
#include "TpccMeta.h"

namespace Cavalia {
	namespace Benchmark {
		namespace Tpcc {
			class TpccIslandConfiguration : public BenchmarkIslandConfiguration {
			public:
				TpccIslandConfiguration(const size_t &core_count, const size_t &partition_count, const size_t &node_id) : BenchmarkIslandConfiguration(core_count, partition_count, node_id) {}
				virtual ~TpccIslandConfiguration() {}

				virtual size_t GetTableCount() const {
					return kTableCount;
				}

			private:
				TpccIslandConfiguration(const TpccIslandConfiguration&);
				TpccIslandConfiguration& operator=(const TpccIslandConfiguration&);
			};
		}
	}
}

#endif
