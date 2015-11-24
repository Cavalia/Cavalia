#pragma once
#ifndef __CAVALIA_DATABASE_FLOW_EXECUTION_PROFILER_H__
#define __CAVALIA_DATABASE_FLOW_EXECUTION_PROFILER_H__

#include <unordered_map>
#include <map>
#include <cstdio>
#if defined(PRECISE_TIMER)
#include <PreciseTimeMeasurer.h>
#else
#include <TimeMeasurer.h>
#endif

#if !defined(MUTE) && defined(PROFILE_FLOW_EXECUTION)
#if defined(PRECISE_TIMER)
#define INIT_FLOW_EXECUTION_TIME_PROFILER \
	core_busy_time_stat_ = new long long[kMaxThreadNum]; \
	for(size_t i = 0; i < kMaxThreadNum; ++i) core_busy_time_stat_[i] = 0; \
	core_wait_time_stat_ = new long long[kMaxThreadNum]; \
	for(size_t i = 0; i < kMaxThreadNum; ++i) core_wait_time_stat_[i] = 0; \
	slice_busy_time_stat_ = new long long*[kMaxThreadNum]; \
	for(size_t i = 0; i < kMaxThreadNum; ++i) { slice_busy_time_stat_[i] = new long long[kMaxSliceNum]; memset(slice_busy_time_stat_[i], 0, kMaxSliceNum * sizeof(long long)); } \
	slice_wait_time_stat_ = new long long*[kMaxThreadNum]; \
	for(size_t i = 0; i < kMaxThreadNum; ++i) { slice_wait_time_stat_[i] = new long long[kMaxSliceNum]; memset(slice_wait_time_stat_[i], 0, kMaxSliceNum * sizeof(long long)); } \
	core_busy_wait_timer_ = new PreciseTimeMeasurer[kMaxThreadNum]; \
	core_execute_timer_ = new PreciseTimeMeasurer[kMaxThreadNum]; \
	core_wait_count_stat_ = new long long[kMaxThreadNum]; \
	for(size_t i = 0; i < kMaxThreadNum; ++i) core_wait_count_stat_[i] = 0;
#else
#define INIT_FLOW_EXECUTION_TIME_PROFILER \
	core_busy_time_stat_ = new long long[kMaxThreadNum]; \
	for(size_t i = 0; i < kMaxThreadNum; ++i) core_busy_time_stat_[i] = 0; \
	core_wait_time_stat_ = new long long[kMaxThreadNum]; \
	for(size_t i = 0; i < kMaxThreadNum; ++i) core_wait_time_stat_[i] = 0; \
	slice_busy_time_stat_ = new long long*[kMaxThreadNum]; \
	for(size_t i = 0; i < kMaxThreadNum; ++i) { slice_busy_time_stat_[i] = new long long[kMaxSliceNum]; memset(slice_busy_time_stat_[i], 0, kMaxSliceNum * sizeof(long long)); } \
	slice_wait_time_stat_ = new long long*[kMaxThreadNum]; \
	for(size_t i = 0; i < kMaxThreadNum; ++i) { slice_wait_time_stat_[i] = new long long[kMaxSliceNum]; memset(slice_wait_time_stat_[i], 0, kMaxSliceNum * sizeof(long long)); } \
	core_busy_wait_timer_ = new TimeMeasurer[kMaxThreadNum]; \
	core_execute_timer_ = new TimeMeasurer[kMaxThreadNum]; \
	core_wait_count_stat_ = new long long[kMaxThreadNum]; \
for(size_t i = 0; i < kMaxThreadNum; ++i) core_wait_count_stat_[i] = 0;
#endif

#define BEGIN_CENTRAL_EXECUTION_TIME_MEASURE \
	central_execution_timer_.StartTimer();

#define END_CENTRAL_EXECUTION_TIME_MEASURE \
	central_execution_timer_.EndTimer(); \
	central_execution_time_stat_ += central_execution_timer_.GetElapsedNanoSeconds();

#define REPORT_FLOW_EXECUTION_TIME_PROFILER(thread_count) \
	printf("**********BEGIN BATCH EXECUTION TIME PROFILING REPORT**********\n"); \
	printf("<<<<<<<<<<<<<<<<<<< CORE TIME PROFILE\n"); \
for (int i = 0; i < kMaxThreadNum; ++i) { \
if(core_busy_time_stat_[i] != 0) { \
	printf("core_id=%d, busy time=%lld ms, waiting time=%lld ms, waiting count=%lld\n", i, core_busy_time_stat_[i] / 1000 / 1000, core_wait_time_stat_[i] / 1000 / 1000, core_wait_count_stat_[i]); \
for (int j = 0; j < kMaxSliceNum; ++j) { \
if (slice_busy_time_stat_[i][j] != 0) { printf("busy slice id=%d, busy slice time=%lld ms\n", j, slice_busy_time_stat_[i][j] / 1000 / 1000); } \
} \
for (int j = 0; j < kMaxSliceNum; ++j) { \
if(slice_wait_time_stat_[i][j] != 0) { printf("wait slice id=%d, wait slice time=%lld ms\n", j, slice_wait_time_stat_[i][j] / 1000 / 1000); } \
} \
} \
} \
for (int slice_id = 0; slice_id < kMaxSliceNum; ++slice_id) { \
	long long total  = 0; \
for (int tid = 0; tid < kMaxThreadNum; ++tid) { \
	total += slice_busy_time_stat_[tid][slice_id]; \
} \
if (total != 0) { printf("slice id=%d, total time=%lld ms\n", slice_id, total / 1000 / 1000 / thread_count); } \
} \
	printf("parameter checker elapsed time = %lld ms\n", central_execution_time_stat_ / 1000 / 1000); \
	printf("**********END BATCH EXECUTION TIME PROFILING REPORT**********\n\n"); 

/*delete[] batch_execution_time_stat_; \
	batch_execution_time_stat_ = NULL; \
	for (size_t i = 0; i < kMaxSliceNum; ++i){ \
	delete[] batch_execution_timer_[i]; \
	batch_execution_timer_[i] = NULL; \
	} \
	delete[] batch_execution_timer_; \
	batch_execution_timer_ = NULL; \
	delete[] core_busy_time_stat_; \
	core_busy_time_stat_ = NULL; \
	delete[] core_busy_wait_timer_; \
	core_busy_wait_timer_ = NULL; \
	delete[] core_wait_time_stat_; \
	core_wait_time_stat_ = NULL; \
	delete[] core_execute_timer_; \
	core_execute_timer_ = NULL;*/

#define BEGIN_CORE_BUSY_TIME_MEASURE(core_id) \
	core_busy_wait_timer_[core_id].StartTimer();

#define END_CORE_BUSY_TIME_MEASURE(core_id, slice_id) \
	core_busy_wait_timer_[core_id].EndTimer(); \
	long long busy_elapsed_time = core_busy_wait_timer_[core_id].GetElapsedNanoSeconds(); \
	core_busy_time_stat_[core_id] += busy_elapsed_time; \
	slice_busy_time_stat_[core_id][slice_id] += busy_elapsed_time;

#define BEGIN_CORE_WAIT_TIME_MEASURE(core_id) \
	core_busy_wait_timer_[core_id].StartTimer();

#define END_CORE_WAIT_TIME_MEASURE(core_id, slice_id) \
	core_busy_wait_timer_[core_id].EndTimer(); \
	long long wait_elapsed_time = core_busy_wait_timer_[core_id].GetElapsedNanoSeconds(); \
	core_wait_time_stat_[core_id] += wait_elapsed_time; \
	slice_wait_time_stat_[core_id][slice_id] += wait_elapsed_time;

#define BEGIN_CORE_EXECUTE_TIME_MEASURE(core_id) \
	core_execute_timer_[core_id].StartTimer();

#define END_CORE_EXECUTE_TIME_MEASURE(core_id) \
	core_execute_timer_[core_id].EndTimer();

#define INC_CORE_WAIT_COUNT(core_id) \
	++core_wait_count_stat_[core_id];
#else
#define INIT_FLOW_EXECUTION_TIME_PROFILER ;
#define REPORT_FLOW_EXECUTION_TIME_PROFILER(thread_count) ;

#define BEGIN_CENTRAL_EXECUTION_TIME_MEASURE ;
#define END_CENTRAL_EXECUTION_TIME_MEASURE ;

#define BEGIN_CORE_BUSY_TIME_MEASURE(core_id);
#define END_CORE_BUSY_TIME_MEASURE(core_id, slice_id);
#define BEGIN_CORE_WAIT_TIME_MEASURE(core_id);
#define END_CORE_WAIT_TIME_MEASURE(core_id, slice_id);
#define BEGIN_CORE_EXECUTE_TIME_MEASURE(core_id);
#define END_CORE_EXECUTE_TIME_MEASURE(core_id);
#define INC_CORE_WAIT_COUNT(core_id);
#endif

namespace Cavalia{
	namespace Database{
		extern long long central_execution_time_stat_;
		extern long long *core_busy_time_stat_;
		extern long long *core_wait_time_stat_;
		extern long long *core_wait_count_stat_;
		extern long long **slice_busy_time_stat_;
		extern long long **slice_wait_time_stat_;

#if defined(PRECISE_TIMER)
		extern PreciseTimeMeasurer central_execution_timer_;
		extern PreciseTimeMeasurer *core_busy_wait_timer_;
		extern PreciseTimeMeasurer *core_execute_timer_;
#else
		extern TimeMeasurer central_execution_timer_;
		extern TimeMeasurer *core_busy_wait_timer_;
		extern TimeMeasurer *core_execute_timer_;
#endif
	}
}
#endif
