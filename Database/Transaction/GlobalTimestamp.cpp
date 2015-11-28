#include "GlobalTimestamp.h"

namespace Cavalia{
	namespace Database{
		std::atomic<uint64_t> GlobalTimestamp::monotone_timestamp_(1);

		std::atomic<uint64_t> *GlobalTimestamp::thread_timestamp_[kMaxThreadNum];
		size_t GlobalTimestamp::thread_count_ = 0;
	}
}
