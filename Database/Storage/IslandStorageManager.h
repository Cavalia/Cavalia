#pragma once
#ifndef __CAVALIA_DATABASE_ISLAND_STORAGE_MANAGER_H__
#define __CAVALIA_DATABASE_ISLAND_STORAGE_MANAGER_H__

#include "BaseStorageManager.h"
#include "IslandTable.h"

namespace Cavalia{
	namespace Database{
		class IslandStorageManager : public BaseStorageManager {
		public:
			IslandStorageManager(const std::string &filename, const IslandTableLocation &table_location, bool is_thread_safe) : BaseStorageManager(filename) {
				table_location_ = table_location;
				is_thread_safe_ = is_thread_safe;
			}
			virtual ~IslandStorageManager(){}

			virtual void RegisterTables(const std::unordered_map<size_t, RecordSchema*> &schemas) = 0;

		private:
			IslandStorageManager(const IslandStorageManager &);
			IslandStorageManager& operator=(const IslandStorageManager &);

		protected:
			// <partition_id, numa_node_id>
			IslandTableLocation table_location_;
			bool is_thread_safe_;
		};
	}
}

#endif
