#pragma once
#ifndef __CAVALIA_BENCHMARK_SCALE_PARAMS_H__
#define __CAVALIA_BENCHMARK_SCALE_PARAMS_H__

#include <string>

namespace Cavalia {
	namespace Benchmark {
			class BenchmarkScaleParams {
			public:
				virtual std::string ToString() const = 0;
			};
	}
}

#endif