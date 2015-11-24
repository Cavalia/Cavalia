#include "ExecutionProfiler.h"

namespace Cavalia{
	namespace Database{
		std::unordered_map<size_t, long long> *execution_stat_;
#if defined(PRECISE_TIMER)
		PreciseTimeMeasurer *execution_timer_;
#else
		TimeMeasurer *execution_timer_;
#endif
	}
}
