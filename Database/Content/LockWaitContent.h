#pragma once
#ifndef __CAVALIA_DATABASE_LOCK_WAIT_CONTENT_H__
#define __CAVALIA_DATABASE_LOCK_WAIT_CONTENT_H__

#include <atomic>
#include <SpinLock.h>
#include "ContentCommon.h"

namespace Cavalia {
	namespace Database {
		class LockWaitContent {
		public:
			LockWaitContent(){
				lock_type_ = NO_LOCK;
				waiters_head_ = NULL;
				owners_head_ = NULL;
				owners_count_ = 0;
				timestamp_ = 0;
			}
			virtual ~LockWaitContent(){
				LockEntry* cur = waiters_head_;
				LockEntry* nxt = NULL;
				while (cur != NULL){
					nxt = cur->next_;
					delete cur;
					cur = nxt;
				}
				cur = owners_head_;
				while (cur != NULL){
					nxt = cur->next_;
					delete cur;
					cur = nxt;
				}
				waiters_head_ = NULL;
			}

			/*true if wait or success, false if abort*/
			bool AcquireLock(const uint64_t& timestamp, const LockType& lock_type, volatile bool* lock_ready){
				bool ret = true;
				AcquireLatch();
				bool is_conflict = Conflict(lock_type, lock_type_);
				if (is_conflict){
					bool can_wait = true;
					// ts should be smaller than all current owners
					LockEntry* entry = owners_head_;
					while (entry != NULL){
						if (entry->timestamp_ < timestamp){
							can_wait = false;
							break;
						}
						entry = entry->next_;
					}
					if (can_wait){
						BufferWaiter(timestamp, lock_type, lock_ready);
						*lock_ready = false;
					}
					else{
						ret = false;
					}
				}
				else{
					if(waiters_head_ != NULL && timestamp < waiters_head_->timestamp_){
						BufferWaiter(timestamp, lock_type, lock_ready);
						*lock_ready = false;
					}
					else{
						// not conflict, add to owner list
						LockEntry* entry = new LockEntry();
						entry->timestamp_ = timestamp;
						entry->next_ = owners_head_;
						owners_head_ = entry;
					
						++owners_count_;
						lock_type_ = lock_type;
					}
				}
				ReleaseLatch();
				return ret;
			}

			void ReleaseLock(const uint64_t& timestamp){
				LockEntry* entry = NULL, *pre = NULL;
				AcquireLatch();
				entry = owners_head_;
				pre = NULL;
				while (entry != NULL && entry->timestamp_ != timestamp){
					pre = entry;
					entry = entry->next_;
				}
				if (entry == NULL){
					// not found, try waiter list
					entry = owners_head_;
					pre = NULL;
					while (entry != NULL && entry->timestamp_ != timestamp){
						pre = entry;
						entry = entry->next_;
					}
					assert(entry != NULL);
					// remove
					if (pre != NULL){
						pre->next_ = entry->next_;
					}
					else{
						waiters_head_ = entry->next_;
					}
					delete entry;
					entry = NULL;
				}
				else{
					// found, remove
					if (pre != NULL){
						pre->next_ = entry->next_;
					}
					else{
						owners_head_ = entry->next_;
					}
					delete entry;
					entry = NULL;

					--owners_count_;
					if (owners_count_ == 0){
						lock_type_ = NO_LOCK;
						DebufferWaiters();
					}
				}

				ReleaseLatch();
				
			}

			void SetTimestamp(const uint64_t &timestamp) {
				assert(timestamp_ <= timestamp);
				timestamp_.store(timestamp, std::memory_order_relaxed);
			}

			uint64_t GetTimestamp() const {
				return timestamp_.load(std::memory_order_relaxed);
			}

		private:
			bool Conflict(const LockType& lt1, const LockType& lt2){
				if (lt1 == NO_LOCK || lt2 == NO_LOCK){
					return false;
				}
				else if (lt1 == READ_LOCK && lt2 == READ_LOCK){
					return false;
				}
				return true;
			}
			void BufferWaiter(const uint64_t& timestamp, const LockType& lock_type, volatile bool* lock_ready){
				LockEntry* entry = waiters_head_;
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
					waiters_head_ = to_add;
				}
			}
			void DebufferWaiters(){
				LockEntry* en = NULL;
				while (waiters_head_ != NULL && !Conflict(waiters_head_->lock_type_, lock_type_)){
					en = waiters_head_;
					waiters_head_ = waiters_head_->next_;
					en->next_ = owners_head_;
					owners_head_ = en;
					++owners_count_;
					lock_type_ = en->lock_type_;
					*(en->lock_ready_) = true;
				}
			}

			void AcquireLatch(){
				spinlock_.Lock();
			}

			void ReleaseLatch(){
				spinlock_.Unlock();
			}

		private:
			// waiters sorted from large to small
			LockEntry* waiters_head_;
			// owner list is not sorted
			LockEntry* owners_head_;
			size_t owners_count_;
			// current holding lock type
			LockType lock_type_;
			SpinLock spinlock_;

			std::atomic<uint64_t> timestamp_;
		};
	}
}

#endif
