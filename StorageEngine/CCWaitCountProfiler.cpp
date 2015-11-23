#include "CCWaitCountProfiler.h"

namespace Cavalia{
	namespace StorageEngine{
		std::unordered_map<size_t, std::unordered_map<size_t, size_t>> *cc_wait_count_;
	}
}