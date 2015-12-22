#pragma once
#ifndef __CAVALIA_MICRO_BENCHMARK_MICRO_SOURCE_H__
#define __CAVALIA_MICRO_BENCHMARK_MICRO_SOURCE_H__

#include <unordered_set>
#include <Benchmark/BenchmarkSource.h>
#include "MicroInformation.h"
#include "MicroRandomGenerator.h"
#include "MicroScaleParams.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Micro{
			class MicroSource : public BenchmarkSource{
			public:
				MicroSource(const std::string &filename_prefix, IORedirector *const redirector, MicroScaleParams *const params, const size_t &num_transactions, const size_t &dist_ratio) : BenchmarkSource(filename_prefix, redirector, params, num_transactions, dist_ratio), num_items_(static_cast<size_t>(params->scalefactor_ * NUM_ITEMS)), random_generator_(num_items_, params->theta_){}

				 virtual ~MicroSource(){}

			private:
				virtual void StartGeneration();

				virtual TxnParam* DeserializeParam(const size_t &param_type, const CharArray &entry){
					TxnParam *tuple;
					if (param_type == TupleType::MICRO){
						tuple = new MicroParam();
					}
					else{
						assert(false);
						return NULL;
					}
					tuple->Deserialize(entry);
					return tuple;
				}

			private:
				MicroSource(const MicroSource &);
				MicroSource& operator=(const MicroSource &);

			private:
				const size_t num_items_;
				MicroRandomGenerator random_generator_;
			};
		}
	}
}

#endif