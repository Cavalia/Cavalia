#include "CCAbortTimeProfiler.h"

namespace Cavalia{
	namespace Database{
		long long *cc_abort_time_stat_;
#if defined(PRECISE_TIMER)
		PreciseTimeMeasurer *abort_timer_;
#else
		TimeMeasurer *cc_abort_timer_;
#endif
	}
}
