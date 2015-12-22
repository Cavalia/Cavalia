#pragma once
#ifndef __CAVALIA_MICRO_BENCHMARK_MICRO_CONSTANTS_H__
#define __CAVALIA_MICRO_BENCHMARK_MICRO_CONSTANTS_H__

namespace Cavalia{
	namespace Benchmark{
		namespace Micro{
			// ----------------------------------------------------------------
			// STORED PROCEDURE EXECUTION FREQUENCIES (0-100)
			// ----------------------------------------------------------------
			const int FREQUENCY_MICRO = 100;

			const int NUM_ITEMS = 1000000;
			const int VALUE_LEN = 64;
			const int NUM_ACCESSES = 10;
		}
	}
}

#endif
