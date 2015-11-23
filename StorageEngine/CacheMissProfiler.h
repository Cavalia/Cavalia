#pragma once
#ifndef __CAVALIA_STORAGE_ENGINE_CACHE_MISS_PROFILER_H__
#define __CAVALIA_STORAGE_ENGINE_CACHE_MISS_PROFILER_H__

#include "MetaTypes.h"
#if defined(PROFILE_CACHE_MISS)
#include <papi.h>
#endif

#if !defined(MUTE) && defined(PROFILE_CACHE_MISS)
#define BEGIN_CACHE_MISS_PROFILE \
	if(PAPI_start_counters(papi_events_, kEventsNum) != PAPI_OK){ \
		std::cout << "PAPI start counters fail !!" << std::endl; \
	}
#define END_CACHE_MISS_PROFILE \
	if(PAPI_stop_counters(papi_values_, kEventsNum) != PAPI_OK){ \
		std::cout << "PAPI end counters fail !!" << std::endl; \
	} \
	std::cout << "***************************************cache profile info***********************************" << std::endl; \
	std::cout << "L1 cache: " << papi_values_[0] << std::endl << "L2 cache: " << papi_values_[1] << std::endl;

#else
#define BEGIN_CACHE_MISS_PROFILE ;
#define END_CACHE_MISS_PROFILE ;
#endif

namespace Cavalia{
	namespace StorageEngine{
#if defined(PROFILE_CACHE_MISS)
		extern int papi_events_[kEventsNum];
		extern long_long papi_values_[kEventsNum];
#endif
	}
}
#endif
