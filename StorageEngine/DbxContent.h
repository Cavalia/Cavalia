#pragma once
#ifndef __CAVALIA_STORAGE_ENGINE_DBX_CONTENT_H__
#define __CAVALIA_STORAGE_ENGINE_DBX_CONTENT_H__

#include <atomic>

namespace Cavalia {
	namespace StorageEngine {
		class DbxContent {
		public:
			DbxContent() : timestamp_(0) {}

			void SetTimestamp(const uint64_t &timestamp) {
				assert(timestamp_ <= timestamp);
				timestamp_.store(timestamp, std::memory_order_relaxed);
			}

			uint64_t GetTimestamp() const {
				return timestamp_.load(std::memory_order_relaxed);
			}

		private:
			std::atomic<uint64_t> timestamp_;
		};
	}
}

#endif
