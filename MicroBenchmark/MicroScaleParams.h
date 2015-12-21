#pragma once
#ifndef __CAVALIA_MICRO_BENCHMARK_MICRO_SCALE_PARAMS_H__
#define __CAVALIA_MICRO_BENCHMARK_MICRO_SCALE_PARAMS_H__

#include <Benchmark/BenchmarkScaleParams.h>

namespace Cavalia{
	namespace Benchmark{
		namespace Micro{
			class MicroScaleParams : public BenchmarkScaleParams{
			public:
				MicroScaleParams(const double &scalefactor, const double &theta) : scalefactor_(scalefactor), theta_(theta){}

				virtual std::string ToString() const {
					std::string ret;
					ret += std::to_string(scalefactor_);
					ret += "_";
					ret += std::to_string(theta_);
					return ret;
				}

			public:
				const double scalefactor_;
				const double theta_;
			};
		}
	}
}

#endif