#pragma once
#ifndef __CAVALIA_DATABASE_LOCK_RTM_CONTENT_H__
#define __CAVALIA_DATABASE_LOCK_RTM_CONTENT_H__

#include <atomic>
#include <cstdint>
#include <RWLock.h>

namespace Cavalia {
	namespace Database {
		class LockRtmContent {
		public:
			LockRtmContent() : timestamp_(0), counter_(0), is_hot_(false) {}

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

			void SetTimestamp(const uint64_t &timestamp) {
				assert(timestamp_ <= timestamp);
				timestamp_.store(timestamp, std::memory_order_relaxed);
			}

			uint64_t GetTimestamp() const {
				return timestamp_.load(std::memory_order_relaxed);
			}

			size_t GetCounter() const {
				return counter_.load(std::memory_order_relaxed);
			}

			size_t IncrementCounter() {
				return counter_.fetch_add(1, std::memory_order_relaxed);
			}

			size_t DecrementCounter() {
				return counter_.fetch_sub(1, std::memory_order_relaxed);
			}

			bool IsHot() const {
				return is_hot_;
			}

			void SetHot(bool is_hot) {
				is_hot_ = is_hot;
			}

		private:
			std::atomic<uint64_t> timestamp_;
			std::atomic<size_t> counter_;
			RWLock lock_;
			bool is_hot_;
		};
	}
}

#endif
