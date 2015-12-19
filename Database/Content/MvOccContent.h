#pragma once
#ifndef __CAVALIA_DATABASE_MVOCC_CONTENT_H__
#define __CAVALIA_DATABASE_MVOCC_CONTENT_H__

#include <atomic>
#include <RWLock.h>
#include "ContentCommon.h"
#include "../Transaction/Epoch.h"
#include "../Transaction/GlobalTimestamp.h"

namespace Cavalia{
	namespace Database{
		class MvOccContent{
		public:
			MvOccContent(char *data_ptr) : data_ptr_(data_ptr), timestamp_(0){
				history_head_ = NULL;
				history_tail_ = NULL;
				history_length_ = 0;
			}
			~MvOccContent(){
				MvHistoryEntry* his, *prev_his;
				his = history_head_;
				while (his != NULL){
					prev_his = his;
					his = his->next_;
					delete prev_his;
					prev_his = NULL;
				}
			}

			void ReadAccess(char *&data_ptr){
				spinlock_.AcquireReadLock();
				if (history_head_ == NULL){
					data_ptr = data_ptr_;
				}
				else{
					data_ptr = history_head_->data_ptr_;
				}
				spinlock_.ReleaseReadLock();
			}

			void ReadAccess(const uint64_t &timestamp, char *&data_ptr){
				spinlock_.AcquireReadLock();
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
				spinlock_.ReleaseReadLock();
			}

			// precondition: write lock acquired already
			void WriteAccess(const uint64_t &commit_timestamp, char* data_ptr){
				spinlock_.AcquireWriteLock();
				assert(commit_timestamp > timestamp_);
				timestamp_ = commit_timestamp;
				MvHistoryEntry* entry = new MvHistoryEntry();
				entry->data_ptr_ = data_ptr;
				COMPILER_MEMORY_FENCE;
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
				//CollectGarbage();
				spinlock_.ReleaseWriteLock();
			}

			void AcquireReadLock(){
				wait_lock_.AcquireReadLock();
			}

			void ReleaseReadLock(){
				wait_lock_.ReleaseReadLock();
			}

			void AcquireWriteLock(){
				wait_lock_.AcquireWriteLock();
			}

			void ReleaseWriteLock(){
				wait_lock_.ReleaseWriteLock();
			}

			void SetTimestamp(const uint64_t &timestamp) {
				timestamp_.store(timestamp, std::memory_order_relaxed);
			}

			uint64_t GetTimestamp() const {
				return timestamp_.load(std::memory_order_relaxed);
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
			char* data_ptr_;
			std::atomic<uint64_t> timestamp_;
			RWLock spinlock_;
			RWLock wait_lock_;
			// history list is sorted by timestamp
			// the latest value is pointed by history_head_
			MvHistoryEntry* history_head_;
			MvHistoryEntry* history_tail_;
			size_t history_length_;
		};
	}
}

#endif
