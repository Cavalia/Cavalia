#pragma once
#ifndef __CAVALIA_MICRO_BENCHMARK_MICRO_TABLE_INITIATOR_H__
#define __CAVALIA_MICRO_BENCHMARK_MICRO_TABLE_INITIATOR_H__

#include <Benchmark/BenchmarkTableInitiator.h>
#include "MicroInformation.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Micro{
			class MicroTableInitiator : public BenchmarkTableInitiator{
			public:
				MicroTableInitiator(){}
				virtual ~MicroTableInitiator(){}

				virtual void Initialize(BaseStorageManager *storage_manager){
					std::unordered_map<size_t, RecordSchema*> schema;
					schema[MICRO_TABLE_ID] = MicroSchema::GenerateMicroSchema();
					storage_manager->RegisterTables(schema);
				}

			private:
				MicroTableInitiator(const MicroTableInitiator&);
				MicroTableInitiator& operator=(const MicroTableInitiator&);
			};
		}
	}
}

#endif
