#pragma once
#ifndef __COMMON_THREAD_HELPER_H__
#define __COMMON_THREAD_HELPER_H__

#include <cassert>
#if defined(__linux__)
#include <pthread.h>
#endif

static void PinToCore(size_t core) {
#if defined(__linux__)
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(core, &cpuset);
	int ret = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
	assert(ret == 0);
#endif
}

#endif
