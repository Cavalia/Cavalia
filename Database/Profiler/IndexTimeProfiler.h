#pragma once
#ifndef __CAVALIA_DATABASE_INDEX_TIME_PROFILER_H__
#define __CAVALIA_DATABASE_INDEX_TIME_PROFILER_H__

#include <cstdio>
#if defined(PRECISE_TIMER)
#include <PreciseTimeMeasurer.h>
#else
#include <TimeMeasurer.h>
#endif

#if !defined(MUTE) && defined(PROFILE_ACCESS_INDEX)
#if defined(PRECISE_TIMER) 
#define INIT_INDEX_TIME_PROFILER \
	access_index_stat_ = new long long[kMaxThreadNum]; \
	for(size_t i = 0; i < kMaxThreadNum; ++i) access_index_stat_[i] = 0; \
	access_index_timer_ = new PreciseTimeMeasurer[kMaxThreadNum];
#else
#define INIT_INDEX_TIME_PROFILER \
	access_index_stat_ = new long long[kMaxThreadNum]; \
	memset(access_index_stat_, 0, sizeof(access_index_stat_)*kMaxThreadNum); \
	access_index_timer_ = new TimeMeasurer[kMaxThreadNum];
#endif
#define BEGIN_INDEX_TIME_MEASURE(thread_id) \
	assert(thread_id==0);\
	access_index_timer_[thread_id].StartTimer();

#define END_INDEX_TIME_MEASURE(thread_id) \
	assert(thread_id==0);\
	access_index_timer_[thread_id].EndTimer(); \
	access_index_stat_[thread_id] += access_index_timer_[thread_id].GetElapsedNanoSeconds();

#define REPORT_INDEX_TIME_PROFILER \
	long long res = 0; \
	for (size_t i = 0; i < kMaxThreadNum; ++i) res += access_index_stat_[i]; \
	printf("********************** ACCESS INDEX TIME REPORT ************\n in total, access index time=%lld ms\n", res / 1000 / 1000); \
	delete[] access_index_timer_; \
	access_index_timer_ = NULL; \
	delete[] access_index_stat_; \
	access_index_stat_ = NULL;

#else
#define INIT_INDEX_TIME_PROFILER ;
#define BEGIN_INDEX_TIME_MEASURE(thread_id) ;
#define END_INDEX_TIME_MEASURE(thread_id) ;
#define REPORT_INDEX_TIME_PROFILER ;
#endif

namespace Cavalia{
	namespace Database{
		extern long long* access_index_stat_;
#if defined(PRECISE_TIMER)
		extern PreciseTimeMeasurer *access_index_timer_;
#else
		extern TimeMeasurer *access_index_timer_;
#endif
	}
}

#endif
