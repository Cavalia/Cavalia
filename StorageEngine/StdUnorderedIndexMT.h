#pragma once
#ifndef __CAVALIA_STORAGE_ENGINE_STD_UNORDERED_INDEX_MT_H__
#define __CAVALIA_STORAGE_ENGINE_STD_UNORDERED_INDEX_MT_H__

#include "StdUnorderedIndex.h"
#include "RWLock.h"

namespace Cavalia {
	namespace StorageEngine {
		class StdUnorderedIndexMT : public StdUnorderedIndex {
		public:
			StdUnorderedIndexMT() {}
			virtual ~StdUnorderedIndexMT() {}

			virtual void InsertRecord(const std::string &key, TableRecord *record) {
				lock_.AcquireWriteLock();
				hash_index_[key] = record;
				lock_.ReleaseWriteLock();
			}

			virtual void DeleteRecord(const std::string &key) {
				lock_.AcquireWriteLock();
				hash_index_.erase(key);
				lock_.ReleaseWriteLock();
			}

			virtual TableRecord* SearchRecord(const std::string &key) {
				lock_.AcquireReadLock();
				if (hash_index_.find(key) == hash_index_.end()) {
					lock_.ReleaseReadLock();
					return NULL;
				}
				else {
					TableRecord *ret_record = hash_index_.at(key);
					lock_.ReleaseReadLock();
					return ret_record;
				}
			}

		private:
			StdUnorderedIndexMT(const StdUnorderedIndexMT &);
			StdUnorderedIndexMT& operator=(const StdUnorderedIndexMT &);

		protected:
			RWLock lock_;
		};
	}
}

#endif
