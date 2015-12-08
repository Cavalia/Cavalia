#pragma once
#ifndef __CAVALIA_DATABASE_SHARD_STORAGE_MANAGER_H__
#define __CAVALIA_DATABASE_SHARD_STORAGE_MANAGER_H__

#include "BaseStorageManager.h"
#include "ShardTable.h"

namespace Cavalia{
	namespace Database{
		class ShardStorageManager : public BaseStorageManager {
		public:
			ShardStorageManager(const std::string &filename, const ShardTableLocation &table_location, bool is_thread_safe) : BaseStorageManager(filename) {
				table_location_ = table_location;
				is_thread_safe_ = is_thread_safe;
			}
			virtual ~ShardStorageManager(){}

			virtual void RegisterTables(const std::unordered_map<size_t, RecordSchema*> &schemas) = 0;

		private:
			ShardStorageManager(const ShardStorageManager &);
			ShardStorageManager& operator=(const ShardStorageManager &);

		protected:
			// <partition_id, numa_node_id>
			ShardTableLocation table_location_;
			bool is_thread_safe_;
		};
	}
}

#endif
