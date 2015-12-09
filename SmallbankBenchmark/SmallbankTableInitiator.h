#pragma once
#ifndef __CAVALIA_SMALLBANK_BENCHMARK_SMALLBANK_TABLE_INITIATOR_H__
#define __CAVALIA_SMALLBANK_BENCHMARK_SMALLBANK_TABLE_INITIATOR_H__

#include <Benchmark/BenchmarkTableInitiator.h>
#include "SmallbankInformation.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Smallbank{
			class SmallbankTableInitiator : public BenchmarkTableInitiator{
			public:
				SmallbankTableInitiator(){}
				virtual ~SmallbankTableInitiator(){}

				virtual void Initialize(BaseStorageManager *storage_manager){
					std::unordered_map<size_t, RecordSchema*> schema;
					schema[ACCOUNTS_TABLE_ID] = SmallbankSchema::GenerateAccountsSchema();
					schema[SAVINGS_TABLE_ID] = SmallbankSchema::GenerateSavingsSchema();
					schema[CHECKING_TABLE_ID] = SmallbankSchema::GenerateCheckingSchema();
					storage_manager->RegisterTables(schema);
				}

			private:
				SmallbankTableInitiator(const SmallbankTableInitiator&);
				SmallbankTableInitiator& operator=(const SmallbankTableInitiator&);
			};
		}
	}
}

#endif
