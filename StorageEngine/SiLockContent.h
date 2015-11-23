#pragma once
#ifndef __CAVALIA_STORAGE_ENGINE_SI_LOCK_CONTENT_H__
#define __CAVALIA_STORAGE_ENGINE_SI_LOCK_CONTENT_H__

#include <boost/thread/mutex.hpp>
#include <boost/atomic.hpp>
#include "MetaTypes.h"
#include "ContentCommon.h"
#include "GlobalContent.h"

namespace Cavalia{
	namespace StorageEngine{
		class SiLockContent{
		public:
			SiLockContent(char *data_ptr) : data_ptr_(data_ptr){
				timestamp_ = 0;
				history_head_ = NULL;
				history_tail_ = NULL;
				history_length_ = 0;
				memset(&lock_, 0, sizeof(lock_));
				memset(&latch_, 0, sizeof(latch_));
			}
			~SiLockContent(){
				MvHistoryEntry* his, *prev_his;
				his = history_head_;
				while (his != NULL){
					prev_his = his;
					his = his->next_;
					delete prev_his;
					prev_his = NULL;
				}
			}

			void ReadAccess(const int64_t &timestamp, char *&data_ptr){
				latch_.lock();
				MvHistoryEntry* entry = history_head_;
				while (entry != NULL){
					if (entry->timestamp_ <= timestamp){
						break;
					}
					entry = entry->next_;
				}
				if (entry == NULL){
					data_ptr = data_ptr_;
				}
				else{
					data_ptr = entry->data_ptr_;
				}
				latch_.unlock();
			}

			// precondition: write lock acquired already
			bool Validate(const int64_t &start_timestamp){
				return start_timestamp >= timestamp_;
			}

			// precondition: write lock acquired already
			void WriteAccess(const int64_t &commit_timestamp, char* data_ptr){
				latch_.lock();
				assert(commit_timestamp > timestamp_);
				timestamp_ = commit_timestamp;
				MvHistoryEntry* entry = new MvHistoryEntry();
				entry->data_ptr_ = data_ptr;
				entry->timestamp_ = commit_timestamp;
				if (history_head_ != NULL){
					entry->next_ = history_head_;
					history_head_->prev_ = entry;
					history_head_ = entry;
				}
				else{
					history_head_ = entry;
					history_tail_ = entry;
				}
				++history_length_;
				CollectGarbage();
				latch_.unlock();
			}

			// set intial ts
			void SetTimestamp(const int64_t &timestamp){
				timestamp_ = timestamp;
			}

			bool AcquireWriteLock(){
				return lock_.try_lock();
			}

			void ReleaseWriteLock(){
				lock_.unlock();
			}

		private:
			void CollectGarbage(){
				if (history_length_ > kRecycleLength){
					int64_t min_thread_ts = GlobalContent::GetMinTimestamp();
					ClearHistory(min_thread_ts);
				}
			}

			void ClearHistory(const int64_t &timestamp) {
				MvHistoryEntry* his_entry = history_tail_;
				while (his_entry != NULL && his_entry->prev_ != NULL && his_entry->prev_->timestamp_ < timestamp) {
					MvHistoryEntry* tmp_entry = his_entry;
					his_entry = his_entry->prev_;
					delete tmp_entry;
					tmp_entry = NULL;
					--history_length_;
				}
				history_tail_ = his_entry;
				if (history_tail_ != NULL){
					history_tail_->next_ = NULL;
				}
			}

		private:
			// the latest version
			int64_t timestamp_;
			// for synchronization
			//boost::mutex latch_;
			//boost::mutex lock_;
			boost::detail::spinlock latch_;
			boost::detail::spinlock lock_;

			char* data_ptr_;
			// history list is sorted by ts
			MvHistoryEntry* history_head_;
			MvHistoryEntry* history_tail_;
			size_t history_length_;
		};
	}
}

#endif
