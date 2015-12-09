#pragma once
#ifndef __CAVALIA_DATABASE_TPCC_ISLAND_STORAGE_MANAGER_H__
#define __CAVALIA_DATABASE_TPCC_ISLAND_STORAGE_MANAGER_H__

#include <Storage/IslandStorageManager.h>
#include "TpccIslandTable.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Tpcc{
			using namespace Cavalia::Database;
			class TpccIslandStorageManager : public IslandStorageManager {
			public:
				TpccIslandStorageManager(const std::string &filename, const IslandTableLocation &table_location, bool is_thread_safe) : IslandStorageManager(filename, table_location, is_thread_safe){}
				virtual ~TpccIslandStorageManager(){}

				virtual void RegisterTables(const std::unordered_map<size_t, RecordSchema*> &schemas){
					table_count_ = schemas.size();
					tables_ = new BaseTable*[table_count_];
					for (size_t i = 0; i < table_count_; ++i){
						tables_[i] = new TpccIslandTable(schemas.at(i), table_location_, is_thread_safe_);
					}
				}

			private:
				TpccIslandStorageManager(const TpccIslandStorageManager &);
				TpccIslandStorageManager& operator=(const TpccIslandStorageManager &);
			};
		}
	}
}

#endif
