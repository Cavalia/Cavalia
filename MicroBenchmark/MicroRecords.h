#pragma once
#ifndef __CAVALIA_MICRO_BENCHMARK_MICRO_RECORDS_H__
#define __CAVALIA_MICRO_BENCHMARK_MICRO_RECORDS_H__

#include <cstdint>

namespace Cavalia{
	namespace Benchmark{
		namespace Micro{
			struct MicroRecord{
				int64_t key_;
				char value_[64];
			};
		}
	}
}

#endif
