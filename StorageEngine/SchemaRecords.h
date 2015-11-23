#pragma once
#ifndef __CAVALIA_STORAGE_ENGINE_SCHEMA_RECORDS_H__
#define __CAVALIA_STORAGE_ENGINE_SCHEMA_RECORDS_H__

#include "SchemaRecord.h"

namespace Cavalia{
	namespace StorageEngine{
		struct SchemaRecords{
			SchemaRecords(const size_t &max_size) : max_size_(max_size){
				curr_size_ = 0;
				records_ = new SchemaRecord*[max_size];
			}
			~SchemaRecords(){
				delete[] records_;
				records_ = NULL;
			}

			void InsertRecord(SchemaRecord *record) {
				assert(curr_size_ < max_size_);
				records_[curr_size_] = record;
				++curr_size_;
			}

			void Clear() {
				curr_size_ = 0;
			}

			const size_t max_size_;
			size_t curr_size_;
			SchemaRecord **records_;
		};
	}
}

#endif