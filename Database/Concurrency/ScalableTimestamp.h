#pragma once
#ifndef __CAVALIA_DATABASE_SCALABLE_TIMESTAMP_H__
#define __CAVALIA_DATABASE_SCALABLE_TIMESTAMP_H__

#include <cstdint>
#include <ThreadHelper.h>
#include <boost/thread.hpp>

#if defined(__linux__)
#define COMPILER_MEMORY_FENCE asm volatile("" ::: "memory")
#else
#define COMPILER_MEMORY_FENCE
#endif

namespace Cavalia {
	namespace Database {
		class ScalableTimestamp {
		public:
			ScalableTimestamp(const size_t &thread_id) {
				ts_thread_ = new boost::thread(boost::bind(&ScalableTimestamp::Start, this));
				ts_thread_id_ = thread_id;
			}

			~ScalableTimestamp() {
				delete ts_thread_;
				ts_thread_ = NULL;
			}

			static uint64_t GetTimestamp() {
				COMPILER_MEMORY_FENCE;
				uint64_t ret_ts = curr_ts_;
				COMPILER_MEMORY_FENCE;
				return ret_ts;
			}

		private:
			void Start() {
				PinToCore(ts_thread_id_ + 1);
				while (true) {
					boost::this_thread::sleep(boost::posix_time::milliseconds(40));
					++curr_ts_;
				}
			}

		private:
			static volatile uint64_t curr_ts_;
			boost::thread *ts_thread_;
			size_t ts_thread_id_;
		};
	}
}

#endif