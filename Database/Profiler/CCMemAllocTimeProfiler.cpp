#include "CCMemAllocTimeProfiler.h"

namespace Cavalia{
	namespace Database{
		long long *cc_mem_alloc_time_stat_;
#if defined(PRECISE_TIMER)
		PreciseTimeMeasurer *cc_mem_alloc_timer_;
#else
		TimeMeasurer *cc_mem_alloc_timer_;
#endif
	}
}