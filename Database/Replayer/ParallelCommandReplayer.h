#pragma once
#ifndef __CAVALIA_DATABASE_PARALLEL_COMMAND_REPLAYER_H__
#define __CAVALIA_DATABASE_PARALLEL_COMMAND_REPLAYER_H__

#include "../Scheduler/ScheduleGraph.h"
#include "../Scheduler/DynamicScheduler.h"
#include "BaseCommandReplayer.h"


#define ALLOCATE_SLICE(Slice, name)\
	Slice *name = (Slice*)MemAllocator::Alloc(sizeof(Slice)); \
	new(name)Slice(); \
	name->SetStorageManager(storage_manager_);

namespace Cavalia{
	namespace Database{
		class ParallelCommandReplayer : public BaseCommandReplayer{
		public:
			ParallelCommandReplayer(const std::string &filename, BaseStorageManager *const storage_manager, const size_t &thread_count) : BaseCommandReplayer(filename, storage_manager, thread_count){}
			virtual ~ParallelCommandReplayer(){}

		private:
			void ProcessLog(){
				boost::thread_group slice_threads;
				for (size_t i = 0; i < thread_count_; ++i){
					slice_threads.create_thread(boost::bind(&ParallelCommandReplayer::RunWorker, this, i));
				}
				slice_threads.join_all();
				assert(scheduler_.IsAllCompleted() == true);
				size_t txn_count = 0;
				for (size_t i = 0; i < layer_count_; ++i) {
					txn_count += execution_batches_.at(i)->size();
				}
				long long elapsed_time = TimeMeasurer::CalcMilliSecondDiff(start_timestamp_, end_timestamp_);
				double throughput = txn_count * 1.0 / elapsed_time;
				double per_core_throughput = throughput / thread_count_;
				std::cout << "thread count=" << thread_count_ << std::endl;
				std::cout << "execute elapsed time=" << elapsed_time << "ms.\nthroughput=" << throughput << "K tps.\nper-core throughput=" << per_core_throughput << "K tps." << std::endl;
			}

			virtual void BuildGraph() = 0;
			virtual void PrepareParams() = 0;
			virtual void RunWorker(const size_t &core_id) = 0;

		private:
			ParallelCommandReplayer(const ParallelCommandReplayer &);
			ParallelCommandReplayer& operator=(const ParallelCommandReplayer &);

		protected:
			ScheduleGraph schedule_graph_;
			DynamicScheduler scheduler_;
		};
	}
}

#endif
