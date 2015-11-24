#pragma once
#ifndef __CAVALIA_DATABASE_EXECUTION_PROFILER_H__
#define __CAVALIA_DATABASE_EXECUTION_PROFILER_H__

#include <unordered_map>
#include <cstdio>
#if defined(PRECISE_TIMER)
#include <PreciseTimeMeasurer.h>
#else
#include <TimeMeasurer.h>
#endif

#if !defined(MUTE) && defined(PROFILE_EXECUTION)
#if defined(PRECISE_TIMER)
#define INIT_EXECUTION_PROFILER \
	execution_stat_ = new std::unordered_map<size_t, long long>[kMaxThreadNum]; \
	execution_timer_ = new PreciseTimeMeasurer[kMaxThreadNum];

#else
#define INIT_EXECUTION_PROFILER \
	execution_stat_ = new std::unordered_map<size_t, long long>[kMaxThreadNum]; \
	execution_timer_ = new TimeMeasurer[kMaxThreadNum];
#endif

#define BEGIN_TRANSACTION_TIME_MEASURE(thread_id) \
	execution_timer_[thread_id].StartTimer();

#define END_TRANSACTION_TIME_MEASURE(thread_id, txn_type) \
	execution_timer_[thread_id].EndTimer(); \
if (execution_stat_[thread_id].find(txn_type) == execution_stat_[thread_id].end()){ \
	execution_stat_[thread_id][txn_type] = execution_timer_[thread_id].GetElapsedNanoSeconds(); \
} \
else{ \
	execution_stat_[thread_id][txn_type] += execution_timer_[thread_id].GetElapsedNanoSeconds(); \
}

#define REPORT_EXECUTION_PROFILER \
	printf("**********BEGIN EXECUTION PROFILING REPORT**********\n"); \
for (int i = 0; i < kMaxThreadNum; ++i){ \
	std::map<size_t, long long> ordered_stat(execution_stat_[i].begin(), execution_stat_[i].end()); \
for (auto &entry : ordered_stat){ \
	printf("thread_id = %d, txn_id = %d, elapsed_time = %lld ms\n", i, (int)entry.first, entry.second / 1000 / 1000); \
} \
} \
	printf("**********END EXECUTION PROFILING REPORT**********\n\n"); \
	delete[] execution_stat_; \
	execution_stat_ = NULL; \
	delete[] execution_timer_; \
	execution_timer_ = NULL;

#else
#define INIT_EXECUTION_PROFILER ;
#define BEGIN_TRANSACTION_TIME_MEASURE(thread_id) ;
#define END_TRANSACTION_TIME_MEASURE(thread_id, txn_type) ;
#define REPORT_EXECUTION_PROFILER ;
#endif

namespace Cavalia{
	namespace Database{
		extern std::unordered_map<size_t, long long> *execution_stat_;
#if defined(PRECISE_TIMER)
		extern PreciseTimeMeasurer *execution_timer_;
#else
		extern TimeMeasurer *execution_timer_;
#endif
	}
}

#endif
