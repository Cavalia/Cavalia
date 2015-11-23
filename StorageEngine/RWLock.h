#pragma once
#ifndef __CAVALIA_STORAGE_ENGINE_RW_LOCK_H__
#define __CAVALIA_STORAGE_ENGINE_RW_LOCK_H__

#include <SpinLock.h>
#include "MetaTypes.h"

namespace Cavalia {
	namespace StorageEngine {
		class RWLock {
		public:
			RWLock() : lock_(NO_LOCK), owner_count_(0) {}

			void AcquireReadLock() {
				while (1) {
					while (lock_ != NO_LOCK && lock_ != READ_LOCK);
					spinlock_.Lock();
					if (lock_ != NO_LOCK && lock_ != READ_LOCK) {
						spinlock_.Unlock();
					}
					else {
						if (lock_ == NO_LOCK) {
							lock_ = READ_LOCK;
							++owner_count_;
						}
						else {
							assert(lock_ == READ_LOCK);
							++owner_count_;
						}
						spinlock_.Unlock();
						return;
					}
				}
			}

			bool TryReadLock() {
				bool rt = false;
				spinlock_.Lock();
				if (lock_ == NO_LOCK) {
					lock_ = READ_LOCK;
					++owner_count_;
					rt = true;
				}
				else if (lock_ == READ_LOCK) {
					++owner_count_;
					rt = true;
				}
				else {
					rt = false;
				}
				spinlock_.Unlock();
				return rt;
			}

			void AcquireWriteLock() {
				while (1) {
					while (lock_ != NO_LOCK);
					spinlock_.Lock();
					if (lock_ != NO_LOCK) {
						spinlock_.Unlock();
					}
					else {
						assert(lock_ == NO_LOCK);
						lock_ = WRITE_LOCK;
						spinlock_.Unlock();
						return;
					}
				}
			}

			bool TryWriteLock() {
				bool rt = false;
				spinlock_.Lock();
				if (lock_ == NO_LOCK) {
					lock_ = WRITE_LOCK;
					rt = true;
				}
				else {
					rt = false;
				}
				spinlock_.Unlock();
				return rt;
			}

			void ReleaseReadLock() {
				spinlock_.Lock();
				--owner_count_;
				if (owner_count_ == 0) {
					lock_ = NO_LOCK;
				}
				spinlock_.Unlock();
			}

			void ReleaseWriteLock() {
				spinlock_.Lock();
				lock_ = NO_LOCK;
				spinlock_.Unlock();
			}

			bool ExistsWriteLock() const {
				return (lock_ == WRITE_LOCK);
			}

		private:
			SpinLock spinlock_;
			volatile LockType lock_;
			volatile size_t owner_count_;
		};
	}
}

#endif
