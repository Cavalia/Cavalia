#pragma once
#ifndef __CAVALIA_STORAGE_ENGINE_SHARD_STORAGE_MANAGER_H__
#define __CAVALIA_STORAGE_ENGINE_SHARD_STORAGE_MANAGER_H__

#include "BaseStorageManager.h"
#include "ShardTable.h"

namespace Cavalia{
	namespace StorageEngine{
		class ShardStorageManager : public BaseStorageManager {
		public:
			ShardStorageManager(const std::string &filename, const std::vector<TableLocation> &table_locations, bool is_thread_safe) : BaseStorageManager(filename), is_thread_safe_(is_thread_safe){
				table_locations_ = table_locations;
			}
			virtual ~ShardStorageManager(){}

			virtual void RegisterTables(const std::unordered_map<size_t, RecordSchema*> &schemas){
				table_count_ = schemas.size();
				tables_ = new BaseTable*[table_count_];
				for (size_t i = 0; i < table_count_; ++i){
					tables_[i] = new ShardTable(schemas.at(i), table_locations_.at(i), is_thread_safe_);
				}
			}

		private:
			ShardStorageManager(const ShardStorageManager &);
			ShardStorageManager& operator=(const ShardStorageManager &);

		protected:
			// table_id => <partition_id, numa_node_id>
			std::vector<TableLocation> table_locations_;
			bool is_thread_safe_;
		};
	}
}

#endif
