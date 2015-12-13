#pragma once
#ifndef __CAVALIA_DATABASE_SLICE_REPLAYER_H__
#define __CAVALIA_DATABASE_SLICE_REPLAYER_H__

#include <ThreadHelper.h>
#include <unordered_map>
#include "../Transaction/StoredProcedure.h"
#include "../Transaction/TxnParam.h"
#include "../Scheduler/ScheduleGraph.h"
#include "../Scheduler/DynamicScheduler.h"
#include "BaseReplayer.h"


#define ALLOCATE_SLICE(Slice, name)\
	Slice *name = (Slice*)MemAllocator::Alloc(sizeof(Slice)); \
	new(name)Slice(); \
	name->SetStorageManager(storage_manager_);

namespace Cavalia{
	namespace Database{
		class SliceReplayer : public BaseReplayer{
		public:
			SliceReplayer(const std::string &filename, BaseStorageManager *const storage_manager, const size_t &thread_count) : BaseReplayer(filename, storage_manager, thread_count, false){
				log_batches_ = new LogEntries[thread_count_];
			}
			virtual ~SliceReplayer(){
				delete[] log_batches_;
				log_batches_ = NULL;
			}

			virtual void Start(){
				TimeMeasurer timer;
				timer.StartTimer();
				boost::thread_group reloaders;
				for (size_t i = 0; i < thread_count_; ++i){
					reloaders.create_thread(boost::bind(&SliceReplayer::ReloadLog, this, i));
				}
				reloaders.join_all();
				timer.EndTimer();
				std::cout << "Reload log elapsed time=" << timer.GetElapsedMilliSeconds() << "ms." << std::endl;
				timer.StartTimer();
				ReorderLog();
				timer.EndTimer();
				std::cout << "Reorder log elapsed time=" << timer.GetElapsedMilliSeconds() << "ms." << std::endl;
				timer.StartTimer();
				BuildGraph();
				PrepareParams();
				ProcessLog();
				timer.EndTimer();
				std::cout << "Process log elapsed time=" << timer.GetElapsedMilliSeconds() << "ms." << std::endl;
			}

		private:
			virtual void PrepareProcedures() = 0;
			virtual TxnParam* DeserializeParam(const size_t &param_type, const CharArray &entry) = 0;

			void ReloadLog(const size_t &thread_id){
				FILE *infile_ptr = infiles_[thread_id];
				LogEntries &log_batch = log_batches_[thread_id];
				fseek(infile_ptr, 0L, SEEK_END);
				size_t file_size = ftell(infile_ptr);
				rewind(infile_ptr);
				size_t file_pos = 0;
				CharArray entry;
				entry.Allocate(1024);
				int result = 0;
				while (file_pos < file_size){
					size_t param_type;
					result = fread(&param_type, sizeof(param_type), 1, infile_ptr);
					assert(result == 1);
					file_pos += sizeof(param_type);
					uint64_t timestamp;
					result = fread(&timestamp, sizeof(timestamp), 1, infile_ptr);
					assert(result == 1);
					file_pos += sizeof(timestamp);
					result = fread(&entry.size_, sizeof(entry.size_), 1, infile_ptr);
					assert(result == 1);
					file_pos += sizeof(entry.size_);
					result = fread(entry.char_ptr_, 1, entry.size_, infile_ptr);
					assert(result == entry.size_);
					TxnParam* event_tuple = DeserializeParam(param_type, entry);
					if (event_tuple != NULL){
						log_batch.push_back(LogEntry(timestamp, event_tuple));
					}
					file_pos += entry.size_;
				}
				assert(file_pos == file_size);
				entry.Release();
			}

			void ReorderLog(){
				for (size_t i = 0; i < thread_count_; ++i){
					std::cout << "thread id=" << i << ", size=" << log_batches_[i].size() << std::endl;
				}
				LogEntries::iterator *iterators = new LogEntries::iterator[thread_count_];
				for (size_t i = 0; i < thread_count_; ++i){
					iterators[i] = log_batches_[i].begin();
				}
				while (true){
					bool all_finished = true;
					uint64_t min_ts = -1;
					size_t thread_id = SIZE_MAX;
					LogEntries::iterator min_entry;
					for (size_t i = 0; i < thread_count_; ++i){
						if (iterators[i] != log_batches_[i].end()){
							if (min_ts == -1 || iterators[i]->timestamp_ < min_ts){
								min_ts = iterators[i]->timestamp_;
								min_entry = iterators[i];
								thread_id = i;
							}
							all_finished = false;
						}
					}
					if (all_finished == true){
						assert(min_ts == -1);
						break;
					}
					else{
						assert(min_ts != -1);
						ordered_logs_.push_back(*min_entry);
						++iterators[thread_id];
					}
				}
				delete[] iterators;
				iterators = NULL;
				std::cout << "full log size=" << ordered_logs_.size() << std::endl;
			}

			virtual void BuildGraph() = 0;
			virtual void PrepareParams() = 0;

			void ProcessLog(){
				boost::thread_group slice_threads;
				for (size_t i = 0; i < thread_count_; ++i){
					slice_threads.create_thread(boost::bind(&SliceReplayer::RunWorker, this, i));
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

			virtual void RunWorker(const size_t &core_id) = 0;

		private:
			SliceReplayer(const SliceReplayer &);
			SliceReplayer& operator=(const SliceReplayer &);

		protected:
			LogEntries *log_batches_;
			LogEntries ordered_logs_;

			ScheduleGraph schedule_graph_;
			DynamicScheduler scheduler_;
		};
	}
}

#endif
