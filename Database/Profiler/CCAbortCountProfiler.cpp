#include "CCAbortCountProfiler.h"

namespace Cavalia{
	namespace Database{
		std::unordered_map<size_t, std::unordered_map<size_t, size_t>> *cc_abort_count_;
	}
}