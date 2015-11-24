#include "PhaseProfiler.h"

namespace Cavalia{
	namespace Database{
		std::unordered_map<size_t, long long> *phase_stat_;
#if defined(PRECISE_TIMER)
		PreciseTimeMeasurer *phase_timer_;
#else
		TimeMeasurer *phase_timer_;
#endif
	}
}
