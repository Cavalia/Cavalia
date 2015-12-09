#pragma once
#ifndef __CAVALIA_DATABASE_TPCC_SHARD_STORAGE_MANAGER_H__
#define __CAVALIA_DATABASE_TPCC_SHARD_STORAGE_MANAGER_H__

#include <Storage/ShardStorageManager.h>
#include "TpccShardTable.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Tpcc{
			using namespace Cavalia::Database;
			class TpccShardStorageManager : public ShardStorageManager {
			public:
				TpccShardStorageManager(const std::string &filename, const ShardTableLocation &table_location, bool is_thread_safe) : ShardStorageManager(filename, table_location, is_thread_safe){}
				virtual ~TpccShardStorageManager(){}

				virtual void RegisterTables(const std::unordered_map<size_t, RecordSchema*> &schemas){
					table_count_ = schemas.size();
					tables_ = new BaseTable*[table_count_];
					for (size_t i = 0; i < table_count_; ++i){
						tables_[i] = new TpccShardTable(schemas.at(i), table_location_, is_thread_safe_);
					}
				}

			private:
				TpccShardStorageManager(const TpccShardStorageManager &);
				TpccShardStorageManager& operator=(const TpccShardStorageManager &);
			};
		}
	}
}

#endif
