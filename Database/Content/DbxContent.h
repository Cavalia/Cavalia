#pragma once
#ifndef __CAVALIA_DATABASE_DBX_CONTENT_H__
#define __CAVALIA_DATABASE_DBX_CONTENT_H__

#include <atomic>
#include <cstdint>

namespace Cavalia {
	namespace Database {
		class DbxContent {
		public:
			DbxContent() : timestamp_(0), counter_(0) {}

			uint64_t GetTimestamp() const {
				return timestamp_;
			}

			uint64_t IncrementTimestamp() {
				++timestamp_;
				return timestamp_;
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
			uint64_t timestamp_;
			std::atomic<size_t> counter_;
		};
	}
}

#endif
