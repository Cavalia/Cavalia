#pragma once
#ifndef __CAVALIA_DATABASE_BASE_STORAGE_MANAGER_H__
#define __CAVALIA_DATABASE_BASE_STORAGE_MANAGER_H__

#include <unordered_map>
#include <string>
#include <fstream>
#include <TimeMeasurer.h>
#include "RecordSchema.h"
#include "BaseTable.h"

namespace Cavalia{
	namespace Database{
		class BaseStorageManager{
		public:
			BaseStorageManager(const std::string &filename) : filename_(filename){}
			virtual ~BaseStorageManager(){}

			virtual void RegisterTables(const std::unordered_map<size_t, RecordSchema*> &schemas) = 0;
			virtual void DropAllTables(){
				for (size_t i = 0; i < table_count_; ++i){
					delete tables_[i];
					tables_[i] = NULL;
				}
				delete[] tables_;
				tables_ = NULL;
			}

			std::string GetStatisticsString(){
				std::string ret;
				int64_t database_size = 0;
				for (size_t i = 0; i < table_count_; ++i){
					if (tables_[i] != NULL){
						size_t tab_size = tables_[i]->GetTableSize();
						ret += "table_id=";
						ret += std::to_string(i);
						ret += ": ";
						ret += std::to_string(tab_size);
						ret += "\n";
						database_size += tables_[i]->GetRecordSize()*tab_size;
					}
				}
				ret += "database size=";
				ret += std::to_string(database_size*1.0 / 1024 / 1024 / 1024);
				ret += " GB\n";
				return ret;
			}

			void SaveCheckpoint(){
				for (size_t i = 0; i < table_count_; ++i){
					if (tables_[i] != NULL){
						SaveTable(i);
					}
				}
			}

			void ReloadCheckpoint(){
				for (size_t i = 0; i < table_count_; ++i){
					if (tables_[i] != NULL){
						ReloadTable(i);
					}
				}
			}

		private:
			void SaveTable(const size_t &table_id){
				std::ofstream output_file(filename_ + std::to_string(table_id), std::ofstream::binary);
				tables_[table_id]->SaveCheckpoint(output_file);
				output_file.close();
			}

			void ReloadTable(const size_t &table_id){
				std::ifstream input_file(filename_ + std::to_string(table_id), std::ifstream::binary);
				assert(input_file.good() == true);
				tables_[table_id]->ReloadCheckpoint(input_file);
				input_file.close();
			}

		private:
			BaseStorageManager(const BaseStorageManager &);
			BaseStorageManager& operator=(const BaseStorageManager &);

		public:
			const std::string filename_;
			BaseTable **tables_;
			size_t table_count_;
		};
	}
}

#endif
