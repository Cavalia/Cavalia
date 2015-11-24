#pragma once
#ifndef __CAVALIA_DATABASE_PHASE_PROFILER_H__
#define __CAVALIA_DATABASE_PHASE_PROFILER_H__

#include <unordered_map>
#include <map>
#include <cstdio>
#if defined(PRECISE_TIMER)
#include <PreciseTimeMeasurer.h>
#else
#include <TimeMeasurer.h>
#endif

#if !defined(MUTE) && defined(PROFILE_PHASE)
#if defined(PRECISE_TIMER)
#define INIT_PHASE_PROFILER \
	phase_stat_ = new std::unordered_map<size_t, long long>[kMaxThreadNum]; \
	phase_timer_ = new PreciseTimeMeasurer[kMaxThreadNum];
#else
#define INIT_PHASE_PROFILER \
	phase_stat_ = new std::unordered_map<size_t, long long>[kMaxThreadNum]; \
	phase_timer_ = new TimeMeasurer[kMaxThreadNum];
#endif

#define BEGIN_PHASE_MEASURE(thread_id, phase_id) \
	phase_timer_[thread_id].StartTimer();

#define END_PHASE_MEASURE(thread_id, phase_id) \
	phase_timer_[thread_id].EndTimer(); \
if (phase_stat_[thread_id].find(phase_id) == phase_stat_[thread_id].end()){ \
	phase_stat_[thread_id][phase_id] = phase_timer_[thread_id].GetElapsedNanoSeconds(); \
} \
else{ \
	phase_stat_[thread_id][phase_id] += phase_timer_[thread_id].GetElapsedNanoSeconds(); \
}

#define REPORT_PHASE_PROFILER \
	printf("**********BEGIN PHASE PROFILING REPORT**********\n"); \
for (int i = 0; i < kMaxThreadNum; ++i){ \
	std::map<size_t, long long> ordered_stat(phase_stat_[i].begin(), phase_stat_[i].end()); \
for (auto &entry : ordered_stat){ \
	printf("thread_id = %d, phase_id = %d, elapsed_time = %lld ms\n", i, (int)entry.first, entry.second / 1000 / 1000); \
} \
} \
	printf("**********END PHASE PROFILING REPORT**********\n\n"); \
	delete[] phase_stat_; \
	phase_stat_ = NULL; \
	delete[] phase_timer_; \
	phase_timer_ = NULL;


#else
#define INIT_PHASE_PROFILER ;
#define BEGIN_PHASE_MEASURE(thread_id, phase_id) ;
#define END_PHASE_MEASURE(thread_id, phase_id) ;
#define REPORT_PHASE_PROFILER ;
#endif

namespace Cavalia{
	namespace Database{
		enum PhaseType : size_t { INSERT_PHASE, SELECT_PHASE, COMMIT_PHASE, ABORT_PHASE };
		extern std::unordered_map<size_t, long long> *phase_stat_;
#if defined(PRECISE_TIMER)
		extern PreciseTimeMeasurer *phase_timer_;
#else
		extern TimeMeasurer *phase_timer_;
#endif
	}
}

#endif
