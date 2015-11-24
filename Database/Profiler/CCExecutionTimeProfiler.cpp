#include "CCExecutionTimeProfiler.h"

namespace Cavalia{
	namespace Database{
		std::unordered_map<size_t, long long> *cc_execution_time_stat_;
#if defined(PRECISE_TIMER)
		PreciseTimeMeasurer **cc_execution_timer_;
#else
		TimeMeasurer **cc_execution_timer_;
#endif
	}
}
