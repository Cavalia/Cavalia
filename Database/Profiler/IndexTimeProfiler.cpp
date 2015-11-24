#include "IndexTimeProfiler.h"

namespace Cavalia{
	namespace Database{
		long long* access_index_stat_;
#if defined(PRECISE_TIMER)
		PreciseTimeMeasurer *access_index_timer_;
#else
		TimeMeasurer *access_index_timer_;
#endif
	}
}
