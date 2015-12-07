#pragma once
#ifndef __CAVALIA_DATABASE_HSTORE_EXECUTOR_H__
#define __CAVALIA_DATABASE_HSTORE_EXECUTOR_H__

#include <ThreadHelper.h>
#include <NumaHelper.h>
#include <unordered_map>
#include <boost/thread/mutex.hpp>
#include "../Storage/ShareStorageManager.h"
#include "../Transaction/StoredProcedure.h"
#include "../Content/HStoreContent.h"
#include "BaseExecutor.h"
#include "HStoreTxnLocation.h"

namespace Cavalia {
	namespace Database {
		class HStoreExecutor : public BaseExecutor {
		public:
			HStoreExecutor(IORedirector *const redirector, BaseStorageManager *const storage_manager, BaseLogger *const logger, const HStoreTxnLocation &txn_location) : BaseExecutor(redirector, logger, txn_location.GetThreadCount()), storage_manager_(storage_manager), txn_location_(txn_location){
				is_begin_ = false;
				is_finish_ = false;
				total_count_ = 0;
				is_ready_ = new volatile bool[thread_count_];
				for (size_t i = 0; i < thread_count_; ++i) {
					is_ready_[i] = false;
				}
				memset(&time_lock_, 0, sizeof(time_lock_));

				spinlocks_ = new boost::detail::spinlock*[thread_count_];
				for (size_t thread_id = 0; thread_id < txn_location_.GetThreadCount(); ++thread_id){
					size_t core_id = txn_location_.Thread2Core(thread_id);
					size_t node_id = GetNumaNodeId(core_id);
					boost::detail::spinlock *lock = (boost::detail::spinlock*)MemAllocator::AllocNode(sizeof(boost::detail::spinlock), node_id);
					memset(lock, 0, sizeof(boost::detail::spinlock));
					spinlocks_[thread_id] = lock;
				}
			}

			virtual ~HStoreExecutor() {
				delete[] is_ready_;
				is_ready_ = NULL;

				for (size_t i = 0; i < thread_count_; ++i){
					MemAllocator::FreeNode((char*)(spinlocks_[i]), sizeof(boost::detail::spinlock));
					spinlocks_[i] = NULL;
				}
				delete[] spinlocks_;
				spinlocks_ = NULL;
			}

			void Start() {
				PrepareProcedures();
				ProcessQuery();
			}

		private:
			virtual void PrepareProcedures() = 0;
			virtual TxnParam* DeserializeParam(const size_t &param_type, const CharArray &entry) = 0;

			virtual void ProcessQuery() {
				boost::thread_group thread_group;
				for (size_t thread_id = 0; thread_id < txn_location_.GetThreadCount(); ++thread_id){
					size_t core_id = txn_location_.Thread2Core(thread_id);
					thread_group.create_thread(boost::bind(&HStoreExecutor::ProcessQueryThread, this, thread_id, core_id));
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
				long long elapsed_time = timer_.CalcMilliSecondDiff(start_timestamp_, end_timestamp_);
				double throughput = total_count_ * 1.0 / elapsed_time;
				double per_core_throughput = throughput / thread_count_;
				std::cout << "execute_count=" << total_count_ << std::endl;
				std::cout << "elapsed time=" << elapsed_time << "ms.\nthroughput=" << throughput << "K tps.\nper-core throughput=" << per_core_throughput << "K tps." << std::endl;
			}

			void ProcessQueryThread(const size_t &part_id, const size_t &core_id) {
				// note that core_id is not equal to thread_id.
				PinToCore(core_id);
				/////////////copy parameter to each core.
				std::vector<ParamBatch*> execution_batches;
				std::vector<ParamBatch*> *input_batches = redirector_ptr_->GetParameterBatches(part_id);
				for (size_t i = 0; i < input_batches->size(); ++i) {
					ParamBatch *tuple_batch = input_batches->at(i);
					// copy to local memory.
					ParamBatch *execution_batch = new ParamBatch(gParamBatchSize);
					for (size_t j = 0; j < tuple_batch->size(); ++j) {
						TxnParam *entry = tuple_batch->get(j);
						// copy each parameter.
						CharArray str;
						entry->Serialize(str);
						TxnParam* new_tuple = DeserializeParam(entry->type_, str);
						execution_batch->push_back(new_tuple);
						str.Clear();
						delete entry;
						entry = NULL;
					}
					execution_batches.push_back(execution_batch);
					delete tuple_batch;
					tuple_batch = NULL;
				}
				/////////////////////////////////////////////////
				// prepare local managers.
#if defined(__linux__)
				size_t node_id = numa_node_of_cpu(core_id);
#else
				size_t node_id = 0;
#endif
				HStoreContent content(spinlocks_, thread_count_);
				TransactionManager txn_manager(storage_manager_, logger_, part_id, this->thread_count_);
				StoredProcedure **procedures = new StoredProcedure*[registers_.size()];
				for (auto &entry : registers_){
					procedures[entry.first] = entry.second(node_id);
					procedures[entry.first]->SetTransactionManager(&txn_manager);
					procedures[entry.first]->SetPartitionId(part_id);
					procedures[entry.first]->SetPartitionCount(thread_count_);
				}
				/////////////////////////////////////////////////
				is_ready_[part_id] = true;
				while (is_begin_ == false);
				int count = 0;
				CharArray ret;
				ret.char_ptr_ = new char[1024];
				ExeContext exe_context;
				for (auto &tuples : execution_batches) {
					for (size_t idx = 0; idx < tuples->size(); ++idx) {
						TxnParam *tuple = tuples->get(idx);
						std::set<size_t> part_ids;
						GetPartitionIds(tuple, part_ids);
						content.LockPartitions(part_ids);
						ret.size_ = 0;
						procedures[tuple->type_]->Execute(tuple, ret, exe_context);
						content.UnlockPartitions(part_ids);
						++count;
						if (is_finish_ == true){
							total_count_ += count;
							return;
						}
					}
				}
				time_lock_.lock();
				end_timestamp_ = timer_.GetTimePoint();
				is_finish_ = true;
				time_lock_.unlock();
				total_count_ += count;

				/////////////////////////////////////////////////
				// prepare hstore_content.
				/*for (auto &entry : deregisters_){
					entry.second((char*)(procedures[entry.first]));
					procedures[entry.first] = NULL;
				}
				MemAllocator::Free((char*)(procedures), sizeof(void*)*deregisters_.size());
				procedures = NULL;*/
				/////////////////////////////////////////////////
			}

			virtual void GetPartitionIds(const TxnParam *tuple, std::set<size_t> &part_ids) = 0;

		private:
			HStoreExecutor(const HStoreExecutor &);
			HStoreExecutor& operator=(const HStoreExecutor &);

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
			volatile bool is_finish_;
			std::atomic<size_t> total_count_;
			HStoreTxnLocation txn_location_;
			boost::detail::spinlock **spinlocks_;
		};
	}
}

#endif
