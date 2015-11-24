#pragma once
#ifndef __CAVALIA_DATABASE_STD_ORDERED_INDEX_H__
#define __CAVALIA_DATABASE_STD_ORDERED_INDEX_H__

#include <unordered_map>
#include "BaseOrderedIndex.h"

namespace Cavalia {
	namespace Database {
		class StdOrderedIndex : public BaseOrderedIndex {
		public:
			StdOrderedIndex(){}
			virtual ~StdOrderedIndex() {}

			virtual void InsertRecord(const std::string &key, TableRecord *record) {
				index_.insert(std::pair<std::string, TableRecord*>(key, record));
			}

			virtual void DeleteRecord(const std::string &key) {
				index_.erase(key);
			}

			virtual TableRecord* SearchRecord(const std::string &key) {
				if (index_.find(key) == index_.end()) {
					return NULL;
				}
				else {
					return index_.find(key)->second;
				}
			}

			virtual void SearchRecords(const std::string &key, TableRecords *records) {
				auto range = index_.equal_range(key);
				for (auto it = range.first; it != range.second; ++it) {
					records->InsertRecord(it->second);
				}
			}

			virtual void SearchUpperRecords(const std::string &key, TableRecords *records){
				for (auto it = index_.lower_bound(key); it != index_.end(); ++it){
					records->InsertRecord(it->second);
				}
			}

			virtual void SearchLowerRecords(const std::string &key, TableRecords *records){
				for (auto it = index_.begin(); it != index_.upper_bound(key); ++it){
					records->InsertRecord(it->second);
				}
			}

			virtual void SearchRangeRecords(const std::string &lower_key, const std::string &upper_key, TableRecords *records){
				for (auto it = index_.lower_bound(lower_key); it != index_.upper_bound(upper_key); ++it){
					records->InsertRecord(it->second);
				}
			}

		private:
			StdOrderedIndex(const StdOrderedIndex &);
			StdOrderedIndex& operator=(const StdOrderedIndex &);

		protected:
			std::multimap<std::string, TableRecord*> index_;
		};
	}
}

#endif
