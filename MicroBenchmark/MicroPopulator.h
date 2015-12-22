#pragma once
#ifndef __CAVALIA_MICRO_BENCHMARK_MICRO_POPULATOR_H__
#define __CAVALIA_MICRO_BENCHMARK_MICRO_POPULATOR_H__

#include <Benchmark/BenchmarkPopulator.h>

#include "MicroRandomGenerator.h"
#include "MicroScaleParams.h"
#include "MicroInformation.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Micro{
			class MicroPopulator : public BenchmarkPopulator{
			public:
				MicroPopulator(const MicroScaleParams *params, BaseStorageManager *storage_manager) : BenchmarkPopulator(storage_manager), num_items_(static_cast<int>(params->scalefactor_*NUM_ITEMS)){}

				virtual ~MicroPopulator(){}

				virtual void StartPopulate();
				virtual void StartPopulate(const size_t &min_w_id, const size_t &max_w_id){}

			private:
				MicroRecord* GenerateMicroRecord(const int&) const;
				void InsertMicroRecord(const MicroRecord*);

			private:
				MicroPopulator(const MicroPopulator&);
				MicroPopulator& operator=(const MicroPopulator&);
			private:
				const int num_items_;
			};
		}
	}
}

#endif
