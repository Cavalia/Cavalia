#pragma once
#ifndef __CAVALIA_DATABASE_BTREE_INDEX_H__
#define __CAVALIA_DATABASE_BTREE_INDEX_H__

#include <BTree/btree_map.h>
#include "BaseOrderedIndex.h"

namespace Cavalia {
	namespace Database {
		class BtreeIndex : public BaseOrderedIndex {
		public:
			BtreeIndex(){}
			virtual ~BtreeIndex() {}

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
				size_t num = index_.count(key);
				if (num == 0) {
					return;
				}
				else {
					auto range = index_.equal_range(key);
					for (auto it = range.first; it != range.second; ++it) {
						records->InsertRecord(it->second);
					}
					return;
				}
			}


		private:
			BtreeIndex(const BtreeIndex &);
			BtreeIndex& operator=(const BtreeIndex &);

		protected:
			btree::btree_multimap<std::string, TableRecord*> index_;
		};
	}
}

#endif
