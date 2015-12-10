#pragma once
#ifndef __CAVALIA_DATABASE_EPOCH_H__
#define __CAVALIA_DATABASE_EPOCH_H__

#include <cstdint>
#include <boost/thread.hpp>

#if defined(__linux__)
#define COMPILER_MEMORY_FENCE asm volatile("" ::: "memory")
#else
#define COMPILER_MEMORY_FENCE
#endif

namespace Cavalia {
	namespace Database {
		class Epoch {
		public:
			Epoch() {
				ts_thread_ = new boost::thread(boost::bind(&Epoch::Start, this));
			}

			~Epoch() {
				delete ts_thread_;
				ts_thread_ = NULL;
			}

			static uint64_t GetEpoch() {
				COMPILER_MEMORY_FENCE;
				uint64_t ret_ts = curr_epoch_;
				COMPILER_MEMORY_FENCE;
				return ret_ts;
			}

		private:
			void Start() {
				while (true) {
					boost::this_thread::sleep(boost::posix_time::milliseconds(40));
					++curr_epoch_;
				}
			}

		private:
			static volatile uint64_t curr_epoch_;
			boost::thread *ts_thread_;
		};
	}
}

#endif
