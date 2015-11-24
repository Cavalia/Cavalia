#pragma once
#ifndef __CAVALIA_STORAGE_ENGINE_DIST_EXECUTOR_H__
#define __CAVALIA_STORAGE_ENGINE_DIST_EXECUTOR_H__

#include <ThreadHelper.h>
#include <unordered_map>
#include <boost/thread/mutex.hpp>
#include "ShareStorageManager.h"
#include "BaseExecutor.h"
#include "StoredProcedure.h"

namespace Cavalia {
	namespace StorageEngine {
		class DistExecutor : public BaseExecutor {
		public:
			DistExecutor(IORedirector *const redirector, BaseStorageManager *const storage_manager, BaseLogger *const logger, const TxnLocation &txn_location) : BaseExecutor(redirector, logger, txn_location.GetCoreCount()), storage_manager_(storage_manager), txn_location_(txn_location){
				is_begin_ = false;
				is_ready_ = new volatile bool[thread_count_];
				for (size_t i = 0; i < thread_count_; ++i) {
					is_ready_[i] = false;
				}
				memset(&time_lock_, 0, sizeof(time_lock_));
			}
			virtual ~DistExecutor() {
				delete[] is_ready_;
				is_ready_ = NULL;
			}

			void Start() {
				PrepareProcedures();
				ProcessQuery();
			}

		private:
			virtual void PrepareProcedures() = 0;
			virtual EventTuple* DeserializeParam(const size_t &param_type, const CharArray &entry) = 0;

			virtual void ProcessQuery() {
				boost::thread_group thread_group;
				for (size_t part_id = 0; part_id < txn_location_.GetCoreCount(); ++part_id){
					size_t core_id = txn_location_.core_ids_.at(part_id);
					thread_group.create_thread(boost::bind(&DistExecutor::ProcessQueryThread, this, part_id, core_id));
				}
				bool is_all_ready = true;
				while (1) {
					for (size_t i = 0; i < thread_count_; ++i) {
						if (is_ready_[i] == false) {
							is_all_ready = false;
							break;
						}
					}
					if (is_all_ready == true) {
						break;
					}
					is_all_ready = true;
				}
				std::cout << "start processing..." << std::endl;
				is_begin_ = true;
				start_timestamp_ = timer_.GetTimePoint();
				thread_group.join_all();
				std::cout << "execute elapsed time=" << timer_.CalcMilliSecondDiff(start_timestamp_, end_timestamp_) << "ms" << std::endl;
			}

			void ProcessQueryThread(const size_t &part_id, const size_t &core_id) {
				// note that core_id is not equal to thread_id.
				PinToCore(core_id);
				/////////////copy parameter to each core.
				std::vector<InputBatch*> execution_batches;
				std::vector<InputBatch*> *input_batches = redirector_ptr_->GetParameterBatches(part_id);
				for (size_t i = 0; i < input_batches->size(); ++i) {
					InputBatch *tuple_batch = input_batches->at(i);
					// copy to local memory.
					InputBatch *execution_batch = new InputBatch();
					for (auto &entry : *tuple_batch) {
						// copy each parameter.
						CharArray str;
						entry.second->Serialize(str);
						EventTuple* new_tuple = DeserializeParam(entry.first, str);
						execution_batch->push_back(std::make_pair(entry.first, new_tuple));
						str.Clear();
						delete entry.second;
						entry.second = NULL;
					}
					execution_batches.push_back(execution_batch);
					delete tuple_batch;
					tuple_batch = NULL;
				}
				/////////////////////////////////////////////////
				// prepare local managers.
#if defined(NUMA)
				size_t node_id = numa_node_of_cpu(core_id);
#else
				size_t node_id = 0;
#endif
				TransactionManager txn_manager(storage_manager_, logger_, part_id);
				StoredProcedure **procedures = new StoredProcedure*[registers_.size()];
				for (auto &entry : registers_){
					procedures[entry.first] = entry.second(node_id);
					procedures[entry.first]->SetTransactionManager(&txn_manager);
					procedures[entry.first]->SetPartitionId(node_id);
					procedures[entry.first]->SetPartitionCount(txn_location_.node_count_);
				}
				/////////////////////////////////////////////////
				is_ready_[part_id] = true;
				while (is_begin_ == false);
				int count = 0;
				int abort_count = 0;
				std::string ret;
				for (auto &tuples : execution_batches) {
					for (auto &tuple_pair : *tuples) {
						if (procedures[tuple_pair.first]->Execute(tuple_pair.second, ret) == false){
							++abort_count;
							while (procedures[tuple_pair.first]->Execute(tuple_pair.second, ret) == false){
								++abort_count;
							}
						}
						++count;
					}
					time_lock_.lock();
					end_timestamp_ = timer_.GetTimePoint();
					time_lock_.unlock();
				}
				printf("total_count=%d, abort_count=%d, abort_rate=%f\n", count, abort_count, abort_count*1.0 / (count + 1));
				/////////////////////////////////////////////////
				/*for (auto &entry : deregisters_){
				entry.second((char*)(procedures[entry.first]));
				procedures[entry.first] = NULL;
				}
				MemAllocator::Free((char*)(procedures), sizeof(void*)*deregisters_.size());
				procedures = NULL;*/
				/////////////////////////////////////////////////
			}

		private:
			DistExecutor(const DistExecutor &);
			DistExecutor& operator=(const DistExecutor &);

		protected:
			std::unordered_map<size_t, std::function<StoredProcedure*(size_t)>> registers_;
			std::unordered_map<size_t, std::function<void(char*)>> deregisters_;

		private:
			BaseStorageManager *const storage_manager_;
			TimeMeasurer timer_;
			system_clock::time_point start_timestamp_;
			system_clock::time_point end_timestamp_;
			boost::detail::spinlock time_lock_;
			volatile bool *is_ready_;
			volatile bool is_begin_;
			TxnLocation txn_location_;
		};
	}
}

#endif
