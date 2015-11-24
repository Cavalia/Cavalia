#pragma once
#ifndef __CAVALIA_STORAGE_ENGINE_STAGE_PARALLEL_REPLAYER_H__
#define __CAVALIA_STORAGE_ENGINE_STAGE_PARALLEL_REPLAYER_H__

#include "BaseParallelReplayer.h"
#include "StageDynamicScheduler.h"

namespace Cavalia{
	namespace StorageEngine{
		class StageParallelReplayer : public BaseParallelReplayer{
		public:
			StageParallelReplayer(const std::string &filename, BaseStorageManager *const storage_manager, const size_t &thread_count, const size_t &layer_count, const std::string &partition_filename) : BaseParallelReplayer(filename, storage_manager, thread_count, partition_filename), layer_count_(layer_count){}
			virtual ~StageParallelReplayer(){}

		private:
			StageParallelReplayer(const StageParallelReplayer &);
			StageParallelReplayer& operator=(const StageParallelReplayer &);

		protected:
			size_t layer_count_;
			StageDynamicScheduler scheduler_;
		};
	}
}

#endif
