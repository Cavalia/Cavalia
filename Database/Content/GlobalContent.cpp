#include "GlobalContent.h"

namespace Cavalia{
	namespace Database{
		std::atomic<uint64_t> GlobalContent::monotone_timestamp_(1);

		std::atomic<uint64_t> *GlobalContent::thread_timestamp_[kMaxThreadNum];
		size_t GlobalContent::thread_count_ = 0;
	}
}
