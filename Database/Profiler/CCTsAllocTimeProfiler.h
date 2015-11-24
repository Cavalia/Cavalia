#pragma once
#ifndef __CAVALIA_DATABASE_CC_TS_ALLOC_PROFILER_H__
#define __CAVALIA_DATABASE_CC_TS_ALLOC_PROFILER_H__

#include <unordered_map>
#include <cstdio>
#if defined(PRECISE_TIMER)
#include <PreciseTimeMeasurer.h>
#else
#include <TimeMeasurer.h>
#endif

#if !defined(MUTE) && defined(PROFILE_CC_TS_ALLOC)
#if defined(PRECISE_TIMER)
#define INIT_CC_TS_ALLOC_TIME_PROFILER \
	cc_ts_alloc_time_stat_ = new long long[kMaxThreadNum]; \
	memset(cc_ts_alloc_time_stat_, 0, sizeof(long long)*kMaxThreadNum); \
	cc_ts_alloc_timer_ = new PreciseTimeMeasurer[kMaxThreadNum];

#else
#define INIT_CC_TS_ALLOC_TIME_PROFILER \
	cc_ts_alloc_time_stat_ = new long long[kMaxThreadNum]; \
	memset(cc_ts_alloc_time_stat_, 0, sizeof(long long)*kMaxThreadNum); \
	cc_ts_alloc_timer_ = new TimeMeasurer[kMaxThreadNum];
#endif

#define BEGIN_CC_TS_ALLOC_TIME_MEASURE(thread_id) \
	cc_ts_alloc_timer_[thread_id].StartTimer();

#define END_CC_TS_ALLOC_TIME_MEASURE(thread_id) \
	cc_ts_alloc_timer_[thread_id].EndTimer(); \
	cc_ts_alloc_time_stat_[thread_id] += cc_ts_alloc_timer_[thread_id].GetElapsedNanoSeconds();

#define REPORT_CC_TS_ALLOC_TIME_PROFILER \
	printf("**********BEGIN CC TS ALLOC TIME PROFILING REPORT**********\n"); \
for (int i = 0; i < kMaxThreadNum; ++i){ \
if (cc_ts_alloc_time_stat_[i] != 0){ \
	printf("thread_id = %d, elapsed_time = %lld ms\n", i, cc_ts_alloc_time_stat_[i] / 1000 / 1000); \
} \
} \
	printf("**********END CC TS ALLOC TIME PROFILING REPORT**********\n\n"); \
	delete[] cc_ts_alloc_time_stat_; \
	cc_ts_alloc_time_stat_ = NULL; \
	delete[] cc_ts_alloc_timer_; \
	cc_ts_alloc_timer_ = NULL;

#else
#define INIT_CC_TS_ALLOC_TIME_PROFILER ;
#define BEGIN_CC_TS_ALLOC_TIME_MEASURE(thread_id) ;
#define END_CC_TS_ALLOC_TIME_MEASURE(thread_id) ;
#define REPORT_CC_TS_ALLOC_TIME_PROFILER ;
#endif

namespace Cavalia{
	namespace Database{
		extern long long *cc_ts_alloc_time_stat_;
#if defined(PRECISE_TIMER)
		extern PreciseTimeMeasurer *cc_ts_alloc_timer_;
#else
		extern TimeMeasurer *cc_ts_alloc_timer_;
#endif
	}
}

#endif
