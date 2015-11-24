#include "FlowExecutionProfiler.h"

namespace Cavalia{
	namespace Database{
		long long central_execution_time_stat_;
		long long *core_busy_time_stat_;
		long long *core_wait_time_stat_;
		long long *core_wait_count_stat_;
		long long **slice_busy_time_stat_;
		long long **slice_wait_time_stat_;
#if defined(PRECISE_TIMER)
		PreciseTimeMeasurer central_execution_timer_;
		PreciseTimeMeasurer *core_busy_wait_timer_;
		PreciseTimeMeasurer *core_execute_timer_;
#else
		TimeMeasurer central_execution_timer_;
		TimeMeasurer *core_busy_wait_timer_;
		TimeMeasurer *core_execute_timer_;
#endif
	}
}
