#pragma once
#ifndef __CAVALIA_DATABASE_STD_UNORDERED_INDEX_MT_H__
#define __CAVALIA_DATABASE_STD_UNORDERED_INDEX_MT_H__

#include <RWLock.h>
#include "StdUnorderedIndex.h"

namespace Cavalia {
	namespace Database {
		class StdUnorderedIndexMT : public StdUnorderedIndex {
		public:
			StdUnorderedIndexMT() {}
			virtual ~StdUnorderedIndexMT() {}

			virtual bool InsertRecord(const std::string &key, TableRecord *record) {
				lock_.AcquireWriteLock();
				if (hash_index_.find(key) == hash_index_.end()){
					hash_index_[key] = record;
					lock_.ReleaseWriteLock();
					return true;
				}
				else{
					lock_.ReleaseWriteLock();
					return false;
				}
			}

			virtual bool DeleteRecord(const std::string &key) {
				lock_.AcquireWriteLock();
				if (hash_index_.find(key) == hash_index_.end()){
					lock_.ReleaseWriteLock();
					return false;
				}
				else{
					hash_index_.erase(key);
					lock_.ReleaseWriteLock();
					return true;
				}
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
