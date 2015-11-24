#pragma once
#ifndef __CAVALIA_DATABASE_CC_EXECUTION_COUNT_PROFILER_H__
#define __CAVALIA_DATABASE_CC_EXECUTION_COUNT_iROFILER_H__

#include <unordered_map>
#include <map>
#include <cstdio>

#if !defined(MUTE) && defined(PROFILE_CC_EXECUTION_COUNT)
#define INIT_CC_EXECUTION_COUNT_PROFILER \
	cc_execution_count_ = new std::unordered_map<size_t, size_t>[kMaxThreadNum];

#define UPDATE_CC_EXECUTION_COUNT(thread_id, point_id) \
if (cc_execution_count_[thread_id].find(point_id) == cc_execution_count_[thread_id].end()){ \
	cc_execution_count_[thread_id][point_id] = 1; \
} \
else{ \
	cc_execution_count_[thread_id][point_id] += 1; \
}

#define REPORT_CC_EXECUTION_COUNT_PROFILER \
	printf("**********BEGIN CC EXECUTION COUNT PROFILING REPORT**********\n"); \
for (int i = 0; i < kMaxThreadNum; ++i){ \
	std::map<size_t, size_t> ordered_count(cc_execution_count_[i].begin(), cc_execution_count_[i].end()); \
for (auto &entry_count : ordered_count){ \
	printf("thread_id = %d, point_id = %d, count = %d\n", i, (int)entry_count.first, (int)entry_count.second); \
} \
} \
	printf("**********END CC EXECUTION COUNT PROFILING REPORT**********\n\n"); \
	delete[] cc_execution_count_; \
	cc_execution_count_ = NULL;

#else
#define INIT_CC_EXECUTION_COUNT_PROFILER ;
#define UPDATE_CC_EXECUTION_COUNT(thread_id, point_id) ;
#define REPORT_CC_EXECUTION_COUNT_PROFILER ;
#endif

namespace Cavalia{
	namespace Database{
		extern std::unordered_map<size_t, size_t> *cc_execution_count_;
	}
}
#endif
