#include "CCTsAllocTimeProfiler.h"

namespace Cavalia{
	namespace Database{
		long long *cc_ts_alloc_time_stat_;
#if defined(PRECISE_TIMER)
		PreciseTimeMeasurer *cc_ts_alloc_timer_;
#else
		TimeMeasurer *cc_ts_alloc_timer_;
#endif
	}
}