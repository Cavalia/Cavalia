#pragma once
#ifndef __CAVALIA_DATABASE_RTM_CONTENT_H__
#define __CAVALIA_DATABASE_RTM_CONTENT_H__

#include <atomic>
#include <cstdint>

namespace Cavalia {
	namespace Database {
		class RtmContent {
		public:
			RtmContent() : timestamp_(0), counter_(0) {}

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

		private:
			std::atomic<uint64_t> timestamp_;
			std::atomic<size_t> counter_;
		};
	}
}

#endif
