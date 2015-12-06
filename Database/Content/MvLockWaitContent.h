#pragma once
#ifndef __CAVALIA_DATABASE_MV_LOCK_WAIT_CONTENT_H__
#define __CAVALIA_DATABASE_MV_LOCK_WAIT_CONTENT_H__

#include <boost/atomic.hpp>
#include <SpinLock.h>
#include <RWLock.h>
#include "../Transaction/GlobalTimestamp.h"
#include "ContentCommon.h"

namespace Cavalia {
	namespace Database {
		class MvLockWaitContent {
		public:
			MvLockWaitContent(char* data_str) : data_ptr_(data_str){
				history_head_ = NULL;
				history_tail_ = NULL;
				history_length_ = 0;

				is_certifying_ = false;
				is_writing_ = false;
				read_count_ = 0;
				owners_ = NULL;
				waiters_ = NULL;
			}
			~MvLockWaitContent(){
				while (history_head_ != NULL){
					MvHistoryEntry* entry = history_head_;
					history_head_ = history_head_->next_;
					delete entry;
					entry = NULL;
				}
				history_tail_ = NULL;
				while (owners_ != NULL){
					LockEntry* en = owners_;
					owners_ = owners_->next_;
					delete en;
					en = NULL;
				}
				while (waiters_ != NULL){
					LockEntry* en = waiters_;
					waiters_ = waiters_->next_;
					delete en;
					en = NULL;
				}
			}

			void WriteAccess(const uint64_t& commit_timestamp, char* data_str){
				rw_lock_.AcquireWriteLock();
				MvHistoryEntry* en = new MvHistoryEntry();
				en->data_ptr_ = data_str;
				en->timestamp_ = commit_timestamp;
				if (history_head_ != NULL){
					en->next_ = history_head_;
					history_head_->prev_ = en;
					history_head_ = en;
				}
				else{
					history_head_ = history_tail_ = en;
				}
				++history_length_;
				//CollectGarbage();
				rw_lock_.ReleaseWriteLock();
			}
			void ReadAccess(char*& data_ptr){
				rw_lock_.AcquireReadLock();
				if (history_head_ != NULL){
					data_ptr = history_head_->data_ptr_;
				}
				else{
					data_ptr = data_ptr_;
				}
				rw_lock_.ReleaseReadLock();
			}
			void ReadAccess(const uint64_t& timestamp, char*& data_ptr){
				rw_lock_.AcquireReadLock();
				MvHistoryEntry* en = history_head_;
				while (en != NULL && en->timestamp_ > timestamp){
					en = en->next_;
				}
				if (en != NULL){
					data_ptr = en->data_ptr_;
				}
				else{
					data_ptr = data_ptr_;
				}
				rw_lock_.ReleaseReadLock();
			}
			bool AcquireLock(const uint64_t& timestamp, const LockType& lock_type, volatile bool* lock_ready){
				bool ret = true;
				wait_lock_.Lock();
				bool is_conflict = IsConflict(lock_type);
				if (is_conflict){
					bool can_wait = true;
					LockEntry* en = owners_;
					while (en != NULL){
						if (en->timestamp_ < timestamp){
							can_wait = false;
							break;
						}
						en = en->next_;
					}
					if (can_wait){
						*lock_ready = false;
						BufferWaiter(timestamp, lock_type, lock_ready);
					}
					else{
						//abort
						ret = false;
					}
				}
				else{
					if (waiters_ != NULL && timestamp < waiters_->timestamp_ ){
						//wait to avoid violating 'the small ts wait for the big ts' rule
						*lock_ready = false;
						BufferWaiter(timestamp, lock_type, lock_ready);
					}
					else{
						//become owner
						LockEntry* en = new LockEntry();
						en->lock_ready_ = lock_ready;
						en->timestamp_ = timestamp;
						en->lock_type_ = lock_type;
						en->next_ = owners_;
						owners_ = en;
						if (lock_type == READ_LOCK){
							++read_count_;
						}
						else if (lock_type == WRITE_LOCK){
							assert(is_writing_ == false);
							is_writing_ = true;
						}
						else if (lock_type == CERTIFY_LOCK){
							assert(is_writing_ == true);
							is_writing_ = false;
							is_certifying_ = true;
						}
						else{
							assert(false);
						}
					}
				}
				wait_lock_.Unlock();
				return ret;
			}
			void ReleaseLock(const uint64_t& timestamp){
				LockEntry* entry = NULL, *pre = NULL;
				wait_lock_.Lock();
				entry = owners_;
				while (entry != NULL && entry->timestamp_ != timestamp){
					pre = entry;
					entry = entry->next_;
				}
				if (entry != NULL){
					if (pre != NULL){
						pre->next_ = entry->next_;
					}
					else{
						owners_ = entry->next_;
					}
					
					if (entry->lock_type_ == READ_LOCK){
						assert(read_count_ > 0);
						--read_count_;
					}
					else if (entry->lock_type_ == WRITE_LOCK){
						assert(is_writing_ == true);
						is_writing_ = false;
					}
					else if (entry->lock_type_ == CERTIFY_LOCK){
						assert(is_certifying_ == true);
						assert(is_writing_ == false);
						assert(read_count_ == 0);
						is_certifying_ = false;
					}
					else{
						assert(false);
					}
					delete entry;
					entry = NULL;
					// allocate legal requests
					DebufferWaiters();
				}
				else{
					entry = waiters_;
					while (entry != NULL && entry->timestamp_ != timestamp){
						pre = entry;
						entry = entry->next_;
					}
					assert(entry != NULL);
					if (pre != NULL){
						pre->next_ = entry->next_;
					}
					else{
						waiters_ = entry->next_;
					}

					delete entry;
					entry = NULL;
				}
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
			bool IsConflict(const LockType& lock_type){
				if (lock_type == READ_LOCK){
					if (is_certifying_){
						return true;
					}
				}
				else if(lock_type == WRITE_LOCK){
					if (is_writing_ || is_certifying_){
						return true;
					}
				}
				else if (lock_type == CERTIFY_LOCK){
					// certify Lock is only issued at the point of commit
					// only the one acquiring write Lock can issue certify Lock
					assert(is_writing_ == true);
					assert(is_certifying_ == false);
					if (read_count_ != 0){
						return true;
					}
				}
				else{
					assert(false);
				}
				return false;
			}
			
			void BufferWaiter(const uint64_t& timestamp, const LockType& lock_type, volatile bool* lock_ready){
				LockEntry* entry = waiters_;
				LockEntry* pre = NULL;
				while (entry != NULL && timestamp < entry->timestamp_){
					pre = entry;
					entry = entry->next_;
				}
				LockEntry* to_add = new LockEntry();
				to_add->timestamp_ = timestamp;
				to_add->lock_type_ = lock_type;
				to_add->lock_ready_ = lock_ready;
				to_add->next_ = entry;
				if (pre != NULL){
					pre->next_ = to_add;
				}
				else{
					waiters_ = to_add;
				}
			}
			void DebufferWaiters(){
				LockEntry* en = NULL;
				while (waiters_ != NULL && !IsConflict(waiters_->lock_type_)){
					en = waiters_;
					waiters_ = waiters_->next_;
					if (en->lock_type_ == CERTIFY_LOCK){
						//eliminate previously acquired write Lock entry
						LockEntry* tmp = owners_, *pre = NULL;
						while (tmp != NULL && tmp->timestamp_ != en->timestamp_){
							pre = tmp;
							tmp = tmp->next_;
						}
						assert(tmp != NULL);
						if (pre != NULL){
							pre->next_ = tmp->next_;
						}
						else{
							owners_ = tmp->next_;
						}
						delete tmp;
						tmp = NULL;

						assert(is_writing_ == true);
						assert(is_certifying_ == false);
						is_writing_ = false;
						is_certifying_ = true;
					}
					else if (en->lock_type_ == READ_LOCK || en->lock_type_ == WRITE_LOCK){
						if (en->lock_type_ == READ_LOCK){
							++read_count_;
						}
						else{ 
							assert(is_writing_ == false);
							is_writing_ = true;
						}
					}
					else{
						assert(false);
					}
					*(en->lock_ready_) = true;
					en->next_ = owners_;
					owners_ = en;
				}
			}

			RWLock rw_lock_;
			char* data_ptr_;
			MvHistoryEntry* history_head_;
			MvHistoryEntry* history_tail_;
			size_t history_length_;

			SpinLock wait_lock_;
			bool is_certifying_;
			bool is_writing_;
			size_t read_count_;
			LockEntry* owners_;
			// sorted from big(young) to small(old)
			LockEntry* waiters_;
		};
	}
}

#endif
