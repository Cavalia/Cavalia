#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_SMALLBANK_SITE_CONFIGURATION_H__
#define __CAVALIA_TPCC_BENCHMARK_SMALLBANK_SITE_CONFIGURATION_H__

#include <Benchmark/BenchmarkSiteConfiguration.h>
#include "SmallbankMeta.h"

namespace Cavalia {
	namespace Benchmark {
		namespace Smallbank {
			class SmallbankSiteConfiguration : public BenchmarkSiteConfiguration {
			public:
				SmallbankSiteConfiguration(const size_t &core_count, const size_t node_count) : BenchmarkSiteConfiguration(core_count, node_count) {}
				~SmallbankSiteConfiguration() {}

				virtual size_t GetTableCount() const {
					return kTableCount;
				}

			private:
				SmallbankSiteConfiguration(const SmallbankSiteConfiguration&);
				SmallbankSiteConfiguration& operator=(const SmallbankSiteConfiguration&);
			};
		}
	}
}

#endif
