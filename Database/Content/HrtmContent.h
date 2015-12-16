#pragma once
#ifndef __CAVALIA_DATABASE_HRTM_CONTENT_H__
#define __CAVALIA_DATABASE_HRTM_CONTENT_H__

#include <atomic>
#include <cstdint>
#include <RWLock.h>

namespace Cavalia {
	namespace Database {
		class HrtmContent {
		public:
			HrtmContent() : timestamp_(0), counter_(0), is_hot_(false) {}
			HrtmContent(bool is_hot) : timestamp_(0), counter_(0), is_hot_(is_hot) {}

			void AcquireReadLock() {
				lock_.AcquireReadLock();
			}

			void ReleaseReadLock() {
				lock_.ReleaseReadLock();
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

		private:
			std::atomic<uint64_t> timestamp_;
			std::atomic<size_t> counter_;
			RWLock lock_;
			const bool is_hot_;
		};
	}
}

#endif
