#pragma once
#ifndef __CAVALIA_DATABASE_SHARD_STORAGE_MANAGER_H__
#define __CAVALIA_DATABASE_SHARD_STORAGE_MANAGER_H__

#include "BaseStorageManager.h"
#include "ShardTable.h"

namespace Cavalia{
	namespace Database{
		class ShardStorageManager : public BaseStorageManager {
		public:
			ShardStorageManager(const std::string &filename, const TableLocation &table_location, bool is_thread_safe) : BaseStorageManager(filename) {
				table_location_ = table_location;
				is_thread_safe_ = is_thread_safe;
			}
			virtual ~ShardStorageManager(){}

			virtual void RegisterTables(const std::unordered_map<size_t, RecordSchema*> &schemas){
				table_count_ = schemas.size();
				tables_ = new BaseTable*[table_count_];
				for (size_t i = 0; i < table_count_; ++i){
					tables_[i] = new ShardTable(schemas.at(i), table_location_, is_thread_safe_);
				}
			}

		private:
			ShardStorageManager(const ShardStorageManager &);
			ShardStorageManager& operator=(const ShardStorageManager &);

		protected:
			// <partition_id, numa_node_id>
			TableLocation table_location_;
			bool is_thread_safe_;
		};
	}
}

#endif
