#pragma once
#ifndef __CAVALIA_DATABASE_STD_UNORDERED_INDEX_H__
#define __CAVALIA_DATABASE_STD_UNORDERED_INDEX_H__

#include <unordered_map>
#include "BaseUnorderedIndex.h"

namespace Cavalia {
	namespace Database {
		class StdUnorderedIndex : public BaseUnorderedIndex {
		public:
			StdUnorderedIndex() {}
			virtual ~StdUnorderedIndex() {
				// all the records in the table should be deleted by primary index.
				// that is, primary index takes the owership of the data. 
				// TODO: records should be allocated from the ScaleMalloc.
				// at current stage, we do not deconstruct entries.
				// tables will only be deleted when the program exits.
				// as a result, we rely on the OS to reclaim memory.
				//for (size_t partition = 0; partition < partition_num_; ++partition){
				//	for (auto &entry : hash_index_[partition]){
				//		delete entry.second;
				//		entry.second = NULL;
				//	}
				//}
			}

			virtual void InsertRecord(const std::string &key, TableRecord *record) {
				hash_index_[key] = record;
			}

			virtual void DeleteRecord(const std::string &key) {
				hash_index_.erase(key);
			}

			virtual TableRecord* SearchRecord(const std::string &key) {
				if (hash_index_.find(key) == hash_index_.end()) {
					return NULL;
				}
				else {
					return hash_index_.at(key);
				}
			}

			virtual size_t GetSize() const {
				return hash_index_.size();
			}

			virtual void SaveCheckpoint(std::ofstream &out_stream, const size_t &record_size){
				for (auto &entry : hash_index_){
					out_stream.write(entry.second->record_->data_ptr_, record_size);
				}
				out_stream.flush();
			}

		private:
			StdUnorderedIndex(const StdUnorderedIndex &);
			StdUnorderedIndex& operator=(const StdUnorderedIndex &);

		protected:
			std::unordered_map<std::string, TableRecord*> hash_index_;
		};
	}
}

#endif
