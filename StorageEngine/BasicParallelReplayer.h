#pragma once
#ifndef __CAVALIA_STORAGE_ENGINE_BASIC_PARALLEL_REPLAYER_H__
#define __CAVALIA_STORAGE_ENGINE_BASIC_PARALLEL_REPLAYER_H__

#include "BaseParallelReplayer.h"
#include "DynamicScheduler.h"

namespace Cavalia{
	namespace StorageEngine{
		class BasicParallelReplayer : public BaseParallelReplayer{
		public:
			BasicParallelReplayer(const std::string &filename, BaseStorageManager *const storage_manager, const size_t &thread_count, const std::string &partition_filename) : BaseParallelReplayer(filename, storage_manager, thread_count, partition_filename){}
			virtual ~BasicParallelReplayer(){}

		private:
			BasicParallelReplayer(const BasicParallelReplayer &);
			BasicParallelReplayer& operator=(const BasicParallelReplayer &);

		protected:
			DynamicScheduler scheduler_;
		};
	}
}

#endif
