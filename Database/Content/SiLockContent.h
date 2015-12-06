#pragma once
#ifndef __CAVALIA_DATABASE_SI_LOCK_CONTENT_H__
#define __CAVALIA_DATABASE_SI_LOCK_CONTENT_H__

#include <SpinLock.h>
#include <RWLock.h>
#include <boost/atomic.hpp>
#include "../Transaction/GlobalTimestamp.h"
#include "ContentCommon.h"

namespace Cavalia{
	namespace Database{
		class SiLockContent{
		public:
			SiLockContent(char *data_ptr) : data_ptr_(data_ptr){
				timestamp_ = 0;
				history_head_ = NULL;
				history_tail_ = NULL;
				history_length_ = 0;
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

			void ReadAccess(const uint64_t &timestamp, char *&data_ptr){
				latch_.Lock();
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
				latch_.Unlock();
			}

			// precondition: write lock acquired already
			bool Validate(const uint64_t &start_timestamp){
				return start_timestamp >= timestamp_;
			}

			// precondition: write lock acquired already
			void WriteAccess(const uint64_t &commit_timestamp, char* data_ptr){
				latch_.Lock();
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
				latch_.Unlock();
			}

			// set intial ts
			void SetTimestamp(const uint64_t &timestamp){
				timestamp_ = timestamp;
			}

			bool TryWriteLock(){
				return lock_.TryWriteLock();
			}

			void AcquireWriteLock(){
				lock_.AcquireWriteLock();
			}

			void ReleaseWriteLock(){
				lock_.ReleaseWriteLock();
			}


		private:
			void CollectGarbage(){
				if (history_length_ > kRecycleLength){
					uint64_t min_thread_ts = GlobalTimestamp::GetMinTimestamp();
					ClearHistory(min_thread_ts);
				}
			}

			void ClearHistory(const uint64_t &timestamp) {
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
			uint64_t timestamp_;
			// for synchronization
			SpinLock latch_;
			RWLock lock_;

			char* data_ptr_;
			// history list is sorted by ts
			MvHistoryEntry* history_head_;
			MvHistoryEntry* history_tail_;
			size_t history_length_;
		};
	}
}

#endif
