#pragma once
#ifndef __CAVALIA_SMALLBANK_BENCHMARK_RANDOM_GENERATOR_H__
#define __CAVALIA_SMALLBANK_BENCHMARK_RANDOM_GENERATOR_H__

#include <random>
#include <math.h>
#include <string>
#include <ZipfDistribution.h>
#include "SmallbankConstants.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Smallbank{
			class SmallbankRandomGenerator{
			public:
				SmallbankRandomGenerator(const uint64_t& num_accounts, const double &theta) : zipf_dist_(num_accounts, theta){}
				~SmallbankRandomGenerator(){}
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
				//static int GenerateGaussianInteger(int min, int max){
				//	int value = -1;
				//	while (value < 0 || value >= (max - min + 1)) {
				//		double gaussian = (distribution(generator) + 2.0) / 4.0;
				//		value = static_cast<int>(round(gaussian * (max - min + 1)));
				//	}
				//	value += min;
				//	return value;
				//}
			
				static std::string GenerateAccountName(const long &n){
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
				SmallbankRandomGenerator(const SmallbankRandomGenerator&);
				SmallbankRandomGenerator& operator=(const SmallbankRandomGenerator&);
			//private:
			//	static std::default_random_engine generator;
			//	static std::normal_distribution<double> distribution;
			private:
				ZipfDistribution zipf_dist_;
			};
		}
	}
}

#endif