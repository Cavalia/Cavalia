#pragma once
#ifndef __CAVALIA_SMALLBANK_BENCHMARK_SMALLBANK_VALUE_REPLAYER_H__
#define __CAVALIA_SMALLBANK_BENCHMARK_SMALLBANK_VALUE_REPLAYER_H__

#include <Replayer/ValueReplayer.h>
#include "SmallbankMeta.h"
#include "SmallbankSchema.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Smallbank{
			namespace Replayer{
				using namespace Cavalia::Database;
				class SmallbankValueReplayer : public ValueReplayer{
				public:
					SmallbankValueReplayer(const std::string &filename, BaseStorageManager *const storage_manager, const size_t &thread_count) : ValueReplayer(filename, storage_manager, thread_count){}
					virtual ~SmallbankValueReplayer(){}

					virtual RecordSchema *GetRecordSchema(const size_t &table_id){
						RecordSchema *schema_ptr = NULL;
						switch (table_id)
						{
						case ACCOUNTS_TABLE_ID:
							schema_ptr = SmallbankSchema::GenerateAccountsSchema();
							break;
						case SAVINGS_TABLE_ID:
							schema_ptr = SmallbankSchema::GenerateSavingsSchema();
							break;
						case CHECKING_TABLE_ID:
							schema_ptr = SmallbankSchema::GenerateCheckingSchema();
							break;
						default:
							assert(false);
							break;
						}
						return schema_ptr;
					}

				private:
					SmallbankValueReplayer(const SmallbankValueReplayer &);
					SmallbankValueReplayer& operator=(const SmallbankValueReplayer &);
				};
			}
		}
	}
}

#endif