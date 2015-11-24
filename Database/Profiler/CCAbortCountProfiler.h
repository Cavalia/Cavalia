#pragma once
#ifndef __CAVALIA_DATABASE_CC_ABORT_COUNT_PROFILER_H__
#define __CAVALIA_DATABASE_CC_ABORT_COUNT_PROFILER_H__

#include <unordered_map>
#include <map>
#include <cstdio>

#if !defined(MUTE) && defined(PROFILE_CC_ABORT_COUNT)
#define INIT_CC_ABORT_COUNT_PROFILER \
	cc_abort_count_ = new std::unordered_map<size_t, std::unordered_map<size_t, size_t>>[kMaxThreadNum];

#define UPDATE_CC_ABORT_COUNT(thread_id, txn_type, table_id) \
if (cc_abort_count_[thread_id][txn_type].find(table_id) == cc_abort_count_[thread_id][txn_type].end()){ \
	cc_abort_count_[thread_id][txn_type][table_id] = 1; \
} \
else{ \
	cc_abort_count_[thread_id][txn_type][table_id] += 1; \
}

#define REPORT_CC_ABORT_COUNT_PROFILER \
	printf("**********BEGIN CC ABORT COUNT PROFILING REPORT**********\n"); \
for (int i = 0; i < kMaxThreadNum; ++i){ \
	std::map<size_t, std::unordered_map<size_t, size_t>> ordered_outer(cc_abort_count_[i].begin(), cc_abort_count_[i].end()); \
for (auto &entry_outer : ordered_outer){ \
	std::map<size_t, size_t> ordered_inner(entry_outer.second.begin(), entry_outer.second.end()); \
for (auto &entry_inner : ordered_inner){ \
	printf("thread_id = %d, txn_id = %d, table_id = %d, count = %d\n", i, (int)entry_outer.first, (int)entry_inner.first, (int)entry_inner.second); \
} \
} \
} \
	printf("**********END CC ABORT COUNT PROFILING REPORT**********\n\n"); \
	delete[] cc_abort_count_; \
	cc_abort_count_ = NULL;

#else
#define INIT_CC_ABORT_COUNT_PROFILER ;
#define UPDATE_CC_ABORT_COUNT(thread_id, txn_type, table_id) ;
#define REPORT_CC_ABORT_COUNT_PROFILER ;
#endif

namespace Cavalia{
	namespace Database{
		extern std::unordered_map<size_t, std::unordered_map<size_t, size_t>> *cc_abort_count_;
	}
}
#endif
