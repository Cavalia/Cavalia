#pragma once
#ifndef __CAVALIA_MICRO_BENCHMARK_RANDOM_GENERATOR_H__
#define __CAVALIA_MICRO_BENCHMARK_RANDOM_GENERATOR_H__

#include <random>
#include <math.h>
#include <string>
#include <ZipfDistribution.h>
#include "MicroConstants.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Micro{
			class MicroRandomGenerator{
			public:
				MicroRandomGenerator(const uint64_t& num_items, const double &theta) : zipf_dist_(num_items, theta){}
				~MicroRandomGenerator(){}
				//taken from tpcc benchmark.
				//generate integer that falls inside [min, max].
				static int GenerateInteger(const int &min, const int &max){
					// TODO: needs a better, cross-platform random generator!
					if (RAND_MAX <= 32767){
						return ((rand() << 16) + rand()) % (max - min + 1) + min;
					}
					else{
						return rand() % (max - min + 1) + min;
					}
				}
			
				static std::string GenerateValue(const long &n){
					char tmp[65]; // 64 + 1
#ifdef _WIN32
					sprintf_s(tmp, "%064ld", n);
#elif __linux__
					sprintf(tmp, "%064ld", n);
#endif
					return std::string(tmp, 64);
				}
				
				int GenerateZipfNumber(){
					return (int)zipf_dist_.GetNextNumber();
				}

			private:
				MicroRandomGenerator(const MicroRandomGenerator&);
				MicroRandomGenerator& operator=(const MicroRandomGenerator&);

			private:
				ZipfDistribution zipf_dist_;
			};
		}
	}
}

#endif