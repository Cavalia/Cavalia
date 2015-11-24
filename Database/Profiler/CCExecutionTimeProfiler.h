#pragma once
#ifndef __CAVALIA_DATABASE_CC_EXECUTION_PROFILER_H__
#define __CAVALIA_DATABASE_CC_EXECUTION_PROFILER_H__

#include <unordered_map>
#include <map>
#include <cstdio>
#if defined(PRECISE_TIMER)
#include <PreciseTimeMeasurer.h>
#else
#include <TimeMeasurer.h>
#endif

#if !defined(MUTE) && defined(PROFILE_CC_EXECUTION_TIME)
#if defined(PRECISE_TIMER)
#define INIT_CC_EXECUTION_TIME_PROFILER \
	cc_execution_time_stat_ = new std::unordered_map<size_t, long long>[kMaxThreadNum]; \
	cc_execution_timer_ = new PreciseTimeMeasurer*[kMaxThreadNum]; \
	for(size_t i = 0; i < kMaxThreadNum; ++i) \
		cc_execution_timer_[i] = new PreciseTimeMeasurer[kPointTypeNum];
#else
#define INIT_CC_EXECUTION_TIME_PROFILER \
	cc_execution_time_stat_ = new std::unordered_map<size_t, long long>[kMaxThreadNum]; \
	cc_execution_timer_ = new TimeMeasurer*[kMaxThreadNum]; \
	for(size_t i = 0; i < kMaxThreadNum; ++i) \
		cc_execution_timer_[i] = new TimeMeasurer[kPointTypeNum];
#endif

#define BEGIN_CC_EXECUTION_TIME_MEASURE(thread_id, point_id) \
	cc_execution_timer_[thread_id][point_id].StartTimer();

#define END_CC_EXECUTION_TIME_MEASURE(thread_id, point_id) \
	cc_execution_timer_[thread_id][point_id].EndTimer(); \
if (cc_execution_time_stat_[thread_id].find(point_id) == cc_execution_time_stat_[thread_id].end()){ \
	cc_execution_time_stat_[thread_id][point_id] = cc_execution_timer_[thread_id][point_id].GetElapsedNanoSeconds(); \
} \
else{ \
	cc_execution_time_stat_[thread_id][point_id] += cc_execution_timer_[thread_id][point_id].GetElapsedNanoSeconds(); \
}

#define REPORT_CC_EXECUTION_TIME_PROFILER \
	printf("**********BEGIN CC EXECUTION TIME PROFILING REPORT**********\n"); \
for (int i = 0; i < kMaxThreadNum; ++i){ \
	std::map<size_t, long long> ordered_stat(cc_execution_time_stat_[i].begin(), cc_execution_time_stat_[i].end()); \
for (auto &entry : ordered_stat){ \
	printf("thread_id = %d, point_id = %d, elapsed_time = %lld ms\n", i, (int)entry.first, entry.second / 1000 / 1000); \
} \
} \
	printf("**********END CC EXECUTION TIME PROFILING REPORT**********\n\n"); \
	delete[] cc_execution_time_stat_; \
	cc_execution_time_stat_ = NULL; \
	for(size_t i = 0; i < kMaxThreadNum; ++i){ \
		delete[] cc_execution_timer_[i]; \
		cc_execution_timer_[i] = NULL; \
	} \
	delete[] cc_execution_timer_; \
	cc_execution_timer_ = NULL;

#else
#define INIT_CC_EXECUTION_TIME_PROFILER ;
#define BEGIN_CC_EXECUTION_TIME_MEASURE(thread_id, point_id) ;
#define END_CC_EXECUTION_TIME_MEASURE(thread_id, point_id) ;
#define REPORT_CC_EXECUTION_TIME_PROFILER ;
#endif

namespace Cavalia{
	namespace Database{
		static const int kPointTypeNum = 4;
		enum MvToPointType : size_t { SELECT_INDEX_POINT, MVTO_REQUST_WRITE_POINT, MVTO_REQUST_READ_POINT, MVTO_WORKSET_POINT };
		extern std::unordered_map<size_t, long long> *cc_execution_time_stat_;
#if defined(PRECISE_TIMER)
		extern PreciseTimeMeasurer **cc_execution_timer_;
#else
		extern TimeMeasurer **cc_execution_timer_;
#endif
	}
}
#endif
