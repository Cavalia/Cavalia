#pragma once
#ifndef __CAVALIA_STORAGE_ENGINE_FLOW_EXECUTOR_H__
#define __CAVALIA_STORAGE_ENGINE_FLOW_EXECUTOR_H__

#include <Toolkits.h>
#include <TimeMeasurer.h>
#include "BaseStorageManager.h"
#include "BaseExecutor.h"
#include "ScheduleGraph.h"
#include "PipelineDynamicScheduler.h"
#include "StageDynamicScheduler.h"

#define ALLOCATE_SLICE(Slice, name)\
	Slice *name = (Slice*)MemAllocator::AllocNode(sizeof(Slice), node_id); \
	new(name)Slice(); \
	name->SetStorageManager(storage_manager_);

namespace Cavalia {
	namespace StorageEngine {
		class CentralFlowExecutor : public BaseExecutor{
		public:
			CentralFlowExecutor(IORedirector *const redirector, BaseStorageManager *storage_manager, BaseLogger *const logger, const size_t &thread_count, const std::vector<size_t> &slice_counts) : BaseExecutor(redirector, logger, thread_count), storage_manager_(storage_manager), slice_counts_(slice_counts){
				execution_batches_ = *(redirector_ptr_->GetParameterBatches());
				layer_count_ = execution_batches_.size();
				memset(&start_time_lock_, 0, sizeof(start_time_lock_));
				memset(&end_time_lock_, 0, sizeof(end_time_lock_));
			}
			virtual ~CentralFlowExecutor(){}

			virtual void Start(){
				BuildGraph();
				PrepareParams();

				ProcessQuery();
			}

			void ProcessQuery(){
				//TimeMeasurer timer;
				//timer.StartTimer();
				for (size_t i = 0; i < thread_count_; ++i){
					slice_threads_.create_thread(boost::bind(&CentralFlowExecutor::RunWorker, this, i));
				}
				slice_threads_.join_all();
				assert(scheduler_.IsAllCompleted() == true);
				//timer.EndTimer();
				size_t txn_count = 0;
				for (size_t i = 0; i < layer_count_; ++i) {
					txn_count += execution_batches_.at(i)->size();
				}
				long long elapsed_time = TimeMeasurer::CalcMilliSecondDiff(start_timestamp_, end_timestamp_);
				double throughput = txn_count * 1.0 / elapsed_time;
				double per_core_throughput = throughput / thread_count_;
				std::cout<<"thread count="<<thread_count_<<std::endl;
				std::cout << "execute elapsed time=" << elapsed_time << "ms.\nthroughput=" << throughput << "K tps.\nper-core throughput=" << per_core_throughput << "K tps." << std::endl;
			}

			virtual void BuildGraph() = 0;
			virtual void PrepareParams() = 0;
			virtual void RunWorker(const size_t &core_id) = 0;

		private:
			CentralFlowExecutor(const CentralFlowExecutor &);
			CentralFlowExecutor& operator=(const CentralFlowExecutor &);

		protected:
			BaseStorageManager *const storage_manager_;
			ScheduleGraph schedule_graph_;
			PipelineDynamicScheduler scheduler_;
			boost::thread_group slice_threads_;

			std::vector<TupleBatch*> execution_batches_;
			std::vector<size_t> slice_counts_;
			size_t layer_count_;
			system_clock::time_point start_timestamp_;
			system_clock::time_point end_timestamp_;
			boost::detail::spinlock start_time_lock_;
			boost::detail::spinlock end_time_lock_;
			
		};
	}
}

#endif
