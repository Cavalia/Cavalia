#pragma once
#ifndef __CAVALIA_DATABASE_DBX_CONTENT_H__
#define __CAVALIA_DATABASE_DBX_CONTENT_H__

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
				return counter_;
			}

			size_t IncrementCounter() {
				++counter_;
				return counter_;
			}

			size_t DecrementCounter() {
				--counter_;
				return counter_;
			}

		private:
			uint64_t timestamp_;
			std::atomic<size_t> counter_;
		};
	}
}

#endif
