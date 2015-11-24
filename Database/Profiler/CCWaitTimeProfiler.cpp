#include "CCWaitTimeProfiler.h"

namespace Cavalia{
	namespace Database{
		long long *cc_wait_time_stat_;
#if defined(PRECISE_TIMER)
		PreciseTimeMeasurer *cc_wait_timer_;
#else
		TimeMeasurer *cc_wait_timer_;
#endif
	}
}