#pragma once
#ifndef __CAVALIA_DATABASE_MV_LOCK_CONTENT_H__
#define __CAVALIA_DATABASE_MV_LOCK_CONTENT_H__

#include <boost/atomic.hpp>
#include <RWLock.h>
#include "../Transaction/GlobalTimestamp.h"
#include "ContentCommon.h"

namespace Cavalia {
	namespace Database {
		class MvLockContent {
		public:
			MvLockContent(char *data_ptr) : data_ptr_(data_ptr), read_count_(0), is_writing_(false), is_certifying_(false){
				history_head_ = NULL;
				history_tail_ = NULL;
				history_length_ = 0;
			}
			~MvLockContent(){
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
				rw_lock_.AcquireReadLock();
				if (history_head_ == NULL){
					data_ptr = data_ptr_;
				}
				else{
					data_ptr = history_head_->data_ptr_;
				}
				rw_lock_.ReleaseReadLock();
			}

			void ReadAccess(const uint64_t &timestamp, char *&data_ptr){
				rw_lock_.AcquireReadLock();
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
				rw_lock_.ReleaseReadLock();
			}

			void WriteAccess(const uint64_t &commit_timestamp, char* data_ptr){
				rw_lock_.AcquireWriteLock();
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
				rw_lock_.ReleaseWriteLock();
			}

			bool AcquireReadLock() {
				bool rt = true;
				wait_lock_.Lock();
				if (is_certifying_ == true){
					rt = false;
				}
				else{
					++read_count_;
				}
				wait_lock_.Unlock();
				return rt;
			}

			void ReleaseReadLock() {
				wait_lock_.Lock();
				assert(read_count_ > 0);
				--read_count_;
				wait_lock_.Unlock();
			}

			bool AcquireWriteLock() {
				bool rt = true;
				wait_lock_.Lock();
				if (is_writing_ == true || is_certifying_ == true){
					rt = false;
				}
				else{
					is_writing_ = true;
				}
				wait_lock_.Unlock();
				return rt;
			}

			void ReleaseWriteLock() {
				wait_lock_.Lock();
				assert(is_writing_ == true);
				is_writing_ = false;
				wait_lock_.Unlock();
			}

			bool AcquireCertifyLock() {
				bool rt = true;
				wait_lock_.Lock();
				assert(is_writing_ == true);
				assert(is_certifying_ == false);
				if (read_count_ != 0){
					rt = false;
				}
				else{
					is_writing_ = false;
					is_certifying_ = true;
				}
				wait_lock_.Unlock();
				return rt;
			}

			void ReleaseCertifyLock() {
				wait_lock_.Lock();
				assert(is_certifying_ == true);
				is_certifying_ = false;
				wait_lock_.Unlock();
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
			SpinLock wait_lock_;
			RWLock rw_lock_;
			size_t read_count_;
			bool is_writing_;
			bool is_certifying_;
			char* data_ptr_;
			// history list is sorted by timestamp
			MvHistoryEntry* history_head_;
			MvHistoryEntry* history_tail_;
			size_t history_length_;
		};
	}
}

#endif
