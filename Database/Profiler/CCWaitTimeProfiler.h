#pragma once
#ifndef __CAVALIA_DATABASE_CC_WAIT_PROFILER_H__
#define __CAVALIA_DATABASE_CC_WAIT_PROFILER_H__

#include <cstdio>
#if defined(PRECISE_TIMER)
#include <PreciseTimeMeasurer.h>
#else
#include <TimeMeasurer.h>
#endif

#if !defined(MUTE) && defined(PROFILE_CC_WAIT)
#if defined(PRECISE_TIMER)
#define INIT_CC_WAIT_TIME_PROFILER \
	cc_wait_time_stat_ = new long long[kMaxThreadNum]; \
	memset(cc_wait_time_stat_, 0, sizeof(long long)*kMaxThreadNum); \
	cc_wait_timer_ = new PreciseTimeMeasurer[kMaxThreadNum];
#else
#define INIT_CC_WAIT_TIME_PROFILER \
	cc_wait_time_stat_ = new long long[kMaxThreadNum]; \
	memset(cc_wait_time_stat_, 0, sizeof(long long)*kMaxThreadNum); \
	cc_wait_timer_ = new TimeMeasurer[kMaxThreadNum];
#endif

#define BEGIN_CC_WAIT_TIME_MEASURE(thread_id) \
	cc_wait_timer_[thread_id].StartTimer();

#define END_CC_WAIT_TIME_MEASURE(thread_id) \
	cc_wait_timer_[thread_id].EndTimer(); \
	cc_wait_time_stat_[thread_id] += cc_wait_timer_[thread_id].GetElapsedNanoSeconds();

#define REPORT_CC_WAIT_TIME_PROFILER \
	printf("**********BEGIN CC WAIT PROFILING REPORT**********\n"); \
for (int i = 0; i < kMaxThreadNum; ++i){ \
if (cc_wait_time_stat_[i] != 0){ \
printf("thread_id = %d, elapsed_time = %lld ms\n", i, cc_wait_time_stat_[i] / 1000 / 1000); \
} \
} \
	printf("**********END CC WAIT PROFILING REPORT**********\n\n"); \
	delete[] cc_wait_time_stat_; \
	cc_wait_time_stat_ = NULL; \
	delete[] cc_wait_timer_; \
	cc_wait_timer_ = NULL;

#else
#define INIT_CC_WAIT_TIME_PROFILER ;
#define BEGIN_CC_WAIT_TIME_MEASURE(thread_id) ;
#define END_CC_WAIT_TIME_MEASURE(thread_id) ;
#define REPORT_CC_WAIT_TIME_PROFILER ;
#endif

namespace Cavalia{
	namespace Database{
		extern long long *cc_wait_time_stat_;
#if defined(PRECISE_TIMER)
		extern PreciseTimeMeasurer *cc_wait_timer_;
#else
		extern TimeMeasurer *cc_wait_timer_;
#endif
	}
}
#endif
