#include "CacheMissProfiler.h"

namespace Cavalia{
	namespace StorageEngine{
#if defined(PROFILE_CACHE_MISS)
		int papi_events_[kEventsNum] = { PAPI_L1_TCM, PAPI_L2_TCM };
		long_long papi_values_[kEventsNum];
#endif
	}
}
