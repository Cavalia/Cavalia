#pragma once
#ifndef __CAVALIA_SMALLBANK_BENCHMARK_SMALLBANK_HSTORE_CONFIGURATION_H__
#define __CAVALIA_SMALLBANK_BENCHMARK_SMALLBANK_HSTORE_CONFIGURATION_H__

#include <Benchmark/BenchmarkHStoreConfiguration.h>
#include "SmallbankMeta.h"

namespace Cavalia {
	namespace Benchmark {
		namespace Smallbank {
			class SmallbankHStoreConfiguration : public BenchmarkHStoreConfiguration{
			public:
				SmallbankHStoreConfiguration(const size_t &core_count, const size_t &node_count) : BenchmarkHStoreConfiguration(core_count, node_count) {}
				~SmallbankHStoreConfiguration() {}

				virtual size_t GetTableCount() const {
					return kTableCount;
				}

			private:
				SmallbankHStoreConfiguration(const SmallbankHStoreConfiguration&);
				SmallbankHStoreConfiguration& operator=(const SmallbankHStoreConfiguration&);
			};
		}
	}
}

#endif
