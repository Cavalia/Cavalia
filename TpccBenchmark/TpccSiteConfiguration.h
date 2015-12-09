#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_TPCC_SITE_CONFIGURATION_H__
#define __CAVALIA_TPCC_BENCHMARK_TPCC_SITE_CONFIGURATION_H__

#include <Benchmark/BenchmarkSiteConfiguration.h>
#include "TpccMeta.h"

namespace Cavalia {
	namespace Benchmark {
		namespace Tpcc {
			class TpccSiteConfiguration : public BenchmarkSiteConfiguration {
			public:
				TpccSiteConfiguration(const size_t &core_count, const size_t &partition_count) : BenchmarkSiteConfiguration(core_count, partition_count) {}
				virtual ~TpccSiteConfiguration() {}

				virtual size_t GetTableCount() const {
					return kTableCount;
				}

			private:
				TpccSiteConfiguration(const TpccSiteConfiguration&);
				TpccSiteConfiguration& operator=(const TpccSiteConfiguration&);
			};
		}
	}
}

#endif
