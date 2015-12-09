#pragma once
#ifndef __CAVALIA_DATABASE_SMALLBANK_SHARD_TABLE_H__
#define __CAVALIA_DATABASE_SMALLBANK_SHARD_TABLE_H__

#include <Storage/ShardTable.h>
#include "SmallbankMeta.h"

namespace Cavalia {
	namespace Benchmark {
		namespace Smallbank{
			using namespace Cavalia::Database;
			class SmallbankShardTable : public ShardTable {
			public:
				SmallbankShardTable(const RecordSchema * const schema_ptr, const ShardTableLocation &table_location, bool is_thread_safe) : ShardTable(schema_ptr, table_location, is_thread_safe){}
				virtual ~SmallbankShardTable() {}

				virtual size_t GetPartitionId(SchemaRecord *record) const {
					int64_t cust_id = *(int64_t*)(record->GetColumn(0));
					return (cust_id - 1) % partition_count_;
				}

				virtual bool IsReplication() const {
					return false;
				}

			private:
				SmallbankShardTable(const SmallbankShardTable&);
				SmallbankShardTable & operator=(const SmallbankShardTable&);
			};
		}
	}
}

#endif