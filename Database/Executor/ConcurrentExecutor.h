#pragma once
#ifndef __CAVALIA_DATABASE_CONCURRENT_EXECUTOR_H__
#define __CAVALIA_DATABASE_CONCURRENT_EXECUTOR_H__

#include <FastRandom.h>
#include <ThreadHelper.h>
#include <NumaHelper.h>
#include <fstream>
#include <iostream>
#include <unordered_map>
#include "../Profiler/Profilers.h"
#include "../Storage/ShareStorageManager.h"
#include "../Transaction/StoredProcedure.h"
#include "../Transaction/Epoch.h"
#include "BaseExecutor.h"
#if defined(DBX) || defined(RTM) || defined(OCC_RTM) || defined(LOCK_RTM)
#include <RtmLock.h>
#endif

namespace Cavalia{
	namespace Database{

		class ConcurrentExecutor : public BaseExecutor{
		public:
			ConcurrentExecutor(IORedirector *const redirector, BaseStorageManager *const storage_manager, BaseLogger *const logger, const size_t &thread_count) : BaseExecutor(redirector, logger, thread_count), storage_manager_(storage_manager){
				is_begin_ = false;
				is_finish_ = false;
				total_count_ = 0;
				total_abort_count_ = 0;
				is_ready_ = new volatile bool[thread_count_];
				for (size_t i = 0; i < thread_count_; ++i){
					is_ready_[i] = false;
				}
				memset(&time_lock_, 0, sizeof(time_lock_));
			}
			virtual ~ConcurrentExecutor(){
				delete[] is_ready_;
				is_ready_ = NULL;
			}

			virtual void Start(){
				GlobalTimestamp::thread_count_ = thread_count_;
				PrepareProcedures();
				ProcessQuery();
			}

		private:
			virtual void PrepareProcedures() = 0;
			virtual TxnParam* DeserializeParam(const size_t &param_type, const CharArray &entry) = 0;

			virtual void ProcessQuery(){
				boost::thread_group thread_group;
				for (size_t i = 0; i < this->thread_count_; ++i){
					size_t core_id = GetCoreId(i);
					thread_group.create_thread(boost::bind(&ConcurrentExecutor::ProcessQueryThread, this, i, core_id));
				}
				bool is_all_ready = true;
				while (1){
					for (size_t i = 0; i < thread_count_; ++i){
						if (is_ready_[i] == false){
							is_all_ready = false;
							break;
						}
					}
					if (is_all_ready == true){
						break;
					}
					is_all_ready = true;
				}
				// epoch generator.
				Epoch epoch;
				std::cout << "start processing..." << std::endl;
				is_begin_ = true;
				start_timestamp_ = timer_.GetTimePoint();
				thread_group.join_all();
				long long elapsed_time = timer_.CalcMilliSecondDiff(start_timestamp_, end_timestamp_);
				double throughput = total_count_ * 1.0 / elapsed_time;
				double per_core_throughput = throughput / thread_count_;
				std::cout << "execute_count=" << total_count_ <<", abort_count=" << total_abort_count_ <<", abort_rate=" <<  total_abort_count_*1.0 / (total_count_ + 1) << std::endl;
				std::cout << "elapsed time=" << elapsed_time << "ms.\nthroughput=" << throughput << "K tps.\nper-core throughput=" << per_core_throughput << "K tps." << std::endl;
#if defined(DBX) || defined(RTM) || defined(OCC_RTM) || defined(LOCK_RTM)
#if defined(PROFILE_RTM)
				rtm_lock_.Print();
#endif
#endif
			}

			void ProcessQueryThread(const size_t &thread_id, const size_t &core_id){
				// note that core_id is not equal to thread_id.
				PinToCore(core_id);
				/////////////copy parameter to each core.
				std::vector<ParamBatch*> execution_batches;
				std::vector<ParamBatch*> *input_batches = redirector_ptr_->GetParameterBatches(thread_id);
				for (size_t i = 0; i < input_batches->size(); ++i){
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
				size_t node_id = GetNumaNodeId(core_id);
				TransactionManager *txn_manager = new TransactionManager(storage_manager_, logger_, thread_id, this->thread_count_);
#if defined(DBX) || defined(RTM) || defined(OCC_RTM) || defined(LOCK_RTM)
				txn_manager->SetRtmLock(&rtm_lock_);
#endif
				StoredProcedure **procedures = new StoredProcedure*[registers_.size()];
				for (auto &entry : registers_){
					procedures[entry.first] = entry.second(node_id);
					procedures[entry.first]->SetTransactionManager(txn_manager);
				}
#if defined(VALUE_LOGGING) || defined(COMMAND_LOGGING)
				logger_->RegisterThread(thread_id, core_id);
#endif
				/////////////////////////////////////////////////
				fast_random r(9084398309893UL);
				is_ready_[thread_id] = true;
				while (is_begin_ == false);
				int count = 0;
				int abort_count = 0;
				uint32_t backoff_shifts = 0;
				CharArray ret;
				ret.char_ptr_ = new char[1024];
				ExeContext exe_context;
				for (auto &tuples : execution_batches){
					for (size_t idx = 0; idx < tuples->size(); ++idx) {
						TxnParam *tuple = tuples->get(idx);
						//double a = r.next_uniform();
						//if (a < gAdhocRatio*1.0 / 100){
						//	is_adhoc = true;
						//}
						BEGIN_TRANSACTION_TIME_MEASURE(thread_id);
						ret.size_ = 0;
						exe_context.is_retry_ = false;
						if (procedures[tuple->type_]->Execute(tuple, ret, exe_context) == false){
							ret.size_ = 0;
							++abort_count;
							if (is_finish_ == true){
								total_count_ += count;
								total_abort_count_ += abort_count;
								END_TRANSACTION_TIME_MEASURE(thread_id, tuple->type_);
								txn_manager->CleanUp();
								return;
							}
							BEGIN_CC_ABORT_TIME_MEASURE(thread_id);
							exe_context.is_retry_ = true;
							if (backoff_shifts < 63){
								++backoff_shifts;
							}
							uint64_t spins = 1UL << backoff_shifts;
#if defined(BACKOFF)
							spins *= 100;
							while (spins){
								_mm_pause();
								--spins;
							}
#endif
							while (procedures[tuple->type_]->Execute(tuple, ret, exe_context) == false){
								exe_context.is_retry_ = true;
								ret.size_ = 0;
								++abort_count;
								if (is_finish_ == true){
									total_count_ += count;
									total_abort_count_ += abort_count;
									END_CC_ABORT_TIME_MEASURE(thread_id);
									END_TRANSACTION_TIME_MEASURE(thread_id, tuple->type_);
									txn_manager->CleanUp();
									return;
								}
#if defined(BACKOFF)
								uint64_t spins = 1UL << backoff_shifts;
								spins *= 100;
								while (spins){
									_mm_pause();
									--spins;
								}
#endif
							}
							END_CC_ABORT_TIME_MEASURE(thread_id);
						}
						else{
#if defined(BACKOFF)
							backoff_shifts >>= 1;
#endif
						}
						++count;
						END_TRANSACTION_TIME_MEASURE(thread_id, tuple->type_);
						if (is_finish_ == true){
							total_count_ += count;
							total_abort_count_ += abort_count;
							txn_manager->CleanUp();
							return;
						}
					}
				}
				time_lock_.lock();
				end_timestamp_ = timer_.GetTimePoint();
				is_finish_ = true;
				time_lock_.unlock();
				total_count_ += count;
				total_abort_count_ += abort_count;
				txn_manager->CleanUp();
				return;
				/////////////////////////////////////////////////
				/*for (auto &entry : deregisters_){
					entry.second((char*)(procedures[entry.first]));
					procedures[entry.first] = NULL;
				}
				MemAllocator::Free((char*)(procedures), sizeof(void*)*deregisters_.size());
				procedures = NULL;*/
				/////////////////////////////////////////////////
			}
			
			size_t GetCoreId(const size_t &thread_id){
				return thread_id;
			}

		private:
			ConcurrentExecutor(const ConcurrentExecutor &);
			ConcurrentExecutor& operator=(const ConcurrentExecutor &);

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
			std::atomic<size_t> total_abort_count_;
#if defined(DBX) || defined(RTM) || defined(OCC_RTM) || defined(LOCK_RTM)
			RtmLock rtm_lock_;
#endif
		};
	}
}

#endif
