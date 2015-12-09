#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_TPCC_HSTORE_CONFIGURATION_H__
#define __CAVALIA_TPCC_BENCHMARK_TPCC_HSTORE_CONFIGURATION_H__

#include <Benchmark/BenchmarkHStoreConfiguration.h>
#include "TpccMeta.h"

namespace Cavalia {
	namespace Benchmark {
		namespace Tpcc {
			class TpccHStoreConfiguration : public BenchmarkHStoreConfiguration{
			public:
				TpccHStoreConfiguration(const size_t &core_count, const size_t &node_count) : BenchmarkHStoreConfiguration(core_count, node_count) {}
				virtual ~TpccHStoreConfiguration() {}

				virtual size_t GetTableCount() const {
					return kTableCount;
				}

			private:
				TpccHStoreConfiguration(const TpccHStoreConfiguration&);
				TpccHStoreConfiguration& operator=(const TpccHStoreConfiguration&);
			};
		}
	}
}

#endif
