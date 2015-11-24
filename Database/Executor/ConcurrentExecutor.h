#pragma once
#ifndef __CAVALIA_DATABASE_CONCURRENT_EXECUTOR_H__
#define __CAVALIA_DATABASE_CONCURRENT_EXECUTOR_H__

#include <FastRandom.h>
#include <ThreadHelper.h>
#include <fstream>
#include <iostream>
#include <unordered_map>
#include "../Storage/ShareStorageManager.h"
#include "../Profiler/Profilers.h"
#include "../Concurrency/StoredProcedure.h"
#include "../Concurrency/ScalableTimestamp.h"
#include "BaseExecutor.h"
#if defined(DBX)
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

			void Start(){
				GlobalContent::thread_count_ = thread_count_;
				PrepareProcedures();
				ProcessQuery();
			}

		private:
			virtual void PrepareProcedures() = 0;
			virtual EventTuple* DeserializeParam(const size_t &param_type, const CharArray &entry) = 0;

			virtual void ProcessQuery(){
				allocator_->Init(thread_count_);
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
#if defined(LOCK) || defined(OCC) || defined(HEALING) || defined(SILO) || defined(HYBRID) || defined(DBX)
				ScalableTimestamp scalable_ts(thread_count_);
#endif
				std::cout << "start processing..." << std::endl;
				BEGIN_CACHE_MISS_PROFILE;
				is_begin_ = true;
				start_timestamp_ = timer_.GetTimePoint();
				thread_group.join_all();
				END_CACHE_MISS_PROFILE;
				long long elapsed_time = timer_.CalcMilliSecondDiff(start_timestamp_, end_timestamp_);
				double throughput = total_count_ * 1.0 / elapsed_time;
				double per_core_throughput = throughput / thread_count_;
				std::cout << "execute_count=" << total_count_ <<", abort_count=" << total_abort_count_ <<", abort_rate=" <<  total_abort_count_*1.0 / (total_count_ + 1) << std::endl;
				std::cout << "elapsed time=" << elapsed_time << "ms.\nthroughput=" << throughput << "K tps.\nper-core throughput=" << per_core_throughput << "K tps." << std::endl;
			}

			void ProcessQueryThread(const size_t &thread_id, const size_t &core_id){
				// note that core_id is not equal to thread_id.
				PinToCore(core_id);
				allocator_->RegisterThread(thread_id, core_id);
				/////////////copy parameter to each core.
				std::vector<TupleBatch*> execution_batches;
				std::vector<TupleBatch*> *input_batches = redirector_ptr_->GetParameterBatches(thread_id);
				for (size_t i = 0; i < input_batches->size(); ++i){
					TupleBatch *tuple_batch = input_batches->at(i);
					// copy to local memory.
					TupleBatch *execution_batch = new TupleBatch(gTupleBatchSize);
					for (size_t j = 0; j < tuple_batch->size(); ++j) {
						EventTuple *entry = tuple_batch->get(j);
						// copy each parameter.
						CharArray str;
						entry->Serialize(str);
						EventTuple* new_tuple = DeserializeParam(entry->type_, str);
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
#if defined(NUMA)
				size_t node_id = numa_node_of_cpu(core_id);
#else
				size_t node_id = 0;
#endif

#if defined(HEALING)
				TransactionManager *txn_manager = manager_generator_(storage_manager_, logger_, thread_id, node_id);
#else
				TransactionManager *txn_manager = new TransactionManager(storage_manager_, logger_, thread_id, this->thread_count_);
#if defined(DBX)
				txn_manager->SetRtmLock(&rtm_lock_);
#endif
#endif
				StoredProcedure **procedures = new StoredProcedure*[registers_.size()];
				for (auto &entry : registers_){
					procedures[entry.first] = entry.second(node_id);
					procedures[entry.first]->SetTransactionManager(txn_manager);
				}
				/////////////////////////////////////////////////
				fast_random r(9084398309893UL);
				is_ready_[thread_id] = true;
				while (is_begin_ == false);
				int count = 0;
				int abort_count = 0;
				//std::vector<long long> latencies;
				//TimeMeasurer per_txn_timer;
				CharArray ret;
				ret.char_ptr_ = new char[1024];
				ExeContext exe_context;
				for (auto &tuples : execution_batches){
					for (size_t idx = 0; idx < tuples->size(); ++idx) {
						EventTuple *tuple = tuples->get(idx);
						//double a = r.next_uniform();
						//if (a < gAdhocRatio*1.0 / 100){
						//	is_adhoc = true;
						//}
						BEGIN_TRANSACTION_TIME_MEASURE(thread_id);
						//per_txn_timer.StartTimer();
						ret.size_ = 0;
						exe_context.is_retry_ = false;
						if (procedures[tuple->type_]->Execute(tuple, ret, exe_context) == false){
							ret.size_ = 0;
							++abort_count;
							if (is_finish_ == true){
								total_count_ += count;
								total_abort_count_ += abort_count;
								END_TRANSACTION_TIME_MEASURE(thread_id, tuple->type_);
								//PrintToFile(thread_id, latencies);
								return;
							}
							BEGIN_CC_ABORT_TIME_MEASURE(thread_id);
							exe_context.is_retry_ = true;
							while (procedures[tuple->type_]->Execute(tuple, ret, exe_context) == false){
								exe_context.is_retry_ = true;
								ret.size_ = 0;
								++abort_count;
								if (is_finish_ == true){
									total_count_ += count;
									total_abort_count_ += abort_count;
									END_CC_ABORT_TIME_MEASURE(thread_id);
									END_TRANSACTION_TIME_MEASURE(thread_id, tuple->type_);
									//PrintToFile(thread_id, latencies);
									return;
								}
							}
							END_CC_ABORT_TIME_MEASURE(thread_id);
						}
						++count;
						//per_txn_timer.EndTimer();
						//long long latency = per_txn_timer.GetElapsedNanoSeconds();
						/*if (count % 100 == 0){
							latencies.push_back(latency);
						}*/
						END_TRANSACTION_TIME_MEASURE(thread_id, tuple->type_);
						if (is_finish_ == true){
							total_count_ += count;
							total_abort_count_ += abort_count;
							//PrintToFile(thread_id, latencies);
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
				//PrintToFile(thread_id, latencies);
				/////////////////////////////////////////////////
				/*for (auto &entry : deregisters_){
					entry.second((char*)(procedures[entry.first]));
					procedures[entry.first] = NULL;
				}
				MemAllocator::Free((char*)(procedures), sizeof(void*)*deregisters_.size());
				procedures = NULL;*/
				/////////////////////////////////////////////////
			}

			/*void PrintToFile(const size_t &thread_id, std::vector<long long> &latencies){
				std::ofstream outfile(std::to_string(thread_id) + "_latency.txt");
				for (auto &entry : latencies){
					outfile << entry << std::endl;
				}
				outfile.flush();
				outfile.close();
			}*/
			
			size_t GetCoreId(const size_t &thread_id){
				return thread_id;
			}

		private:
			ConcurrentExecutor(const ConcurrentExecutor &);
			ConcurrentExecutor& operator=(const ConcurrentExecutor &);

		protected:
			std::unordered_map<size_t, std::function<StoredProcedure*(size_t)>> registers_;
			std::unordered_map<size_t, std::function<void(char*)>> deregisters_;
#if defined(HEALING)
			std::function<TransactionManager*(BaseStorageManager*const, BaseLogger*const, size_t, size_t)> manager_generator_;
#endif

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
#if defined(DBX)
			RtmLock rtm_lock_;
#endif
		};
	}
}

#endif
