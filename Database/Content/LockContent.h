#pragma once
#ifndef __CAVALIA_DATABASE_LOCK_CONTENT_H__
#define __CAVALIA_DATABASE_LOCK_CONTENT_H__

#include <atomic>
#include <RWLock.h>

namespace Cavalia {
	namespace Database {
		class LockContent {
		public:
			LockContent() : timestamp_(0) {}

			bool TryReadLock() {
				return lock_.TryReadLock();
			}

			void AcquireReadLock() {
				lock_.AcquireReadLock();
			}

			void ReleaseReadLock() {
				lock_.ReleaseReadLock();
			}

			bool TryWriteLock() {
				return lock_.TryWriteLock();
			}

			void AcquireWriteLock() {
				lock_.AcquireWriteLock();
			}

			void ReleaseWriteLock() {
				lock_.ReleaseWriteLock();
			}

			bool ExistsWriteLock() const {
				return lock_.ExistsWriteLock();
			}

			void SetTimestamp(const uint64_t &timestamp) {
				assert(timestamp_ <= timestamp);
				timestamp_.store(timestamp, std::memory_order_relaxed);
			}

			uint64_t GetTimestamp() const {
				return timestamp_.load(std::memory_order_relaxed);
			}

		private:
			std::atomic<uint64_t> timestamp_;
			RWLock lock_;
		};
	}
}

#endif
