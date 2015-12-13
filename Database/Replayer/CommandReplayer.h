#pragma once
#ifndef __CAVALIA_DATABASE_COMMAND_REPLAYER_H__
#define __CAVALIA_DATABASE_COMMAND_REPLAYER_H__

#include <unordered_map>
#include "../Transaction/TransactionManager.h"
#include "../Transaction/StoredProcedure.h"
#include "../Transaction/TxnParam.h"
#include "BaseReplayer.h"

namespace Cavalia{
	namespace Database{
		class CommandReplayer : public BaseReplayer{
		public:
			CommandReplayer(const std::string &filename, BaseStorageManager *const storage_manager, const size_t &thread_count) : BaseReplayer(filename, storage_manager, thread_count, false){
				log_entries_ = new CommandLogEntries[thread_count_];
			}
			virtual ~CommandReplayer(){
				delete[] log_entries_;
				log_entries_ = NULL;
			}

			virtual void Start(){
				TimeMeasurer timer;
				timer.StartTimer();
				boost::thread_group reloaders;
				for (size_t i = 0; i < thread_count_; ++i){
					reloaders.create_thread(boost::bind(&CommandReplayer::ReloadLog, this, i));
				}
				reloaders.join_all();
				timer.EndTimer();
				std::cout << "Reload log elapsed time=" << timer.GetElapsedMilliSeconds() << "ms." << std::endl;
				timer.StartTimer();
				ReorderLog();
				timer.EndTimer();
				std::cout << "Reorder log elapsed time=" << timer.GetElapsedMilliSeconds() << "ms." << std::endl;
				timer.StartTimer();
				ProcessLog();
				timer.EndTimer();
				std::cout << "Process log elapsed time=" << timer.GetElapsedMilliSeconds() << "ms." << std::endl;
			}

		private:
			virtual void PrepareProcedures() = 0;
			virtual TxnParam* DeserializeParam(const size_t &param_type, const CharArray &entry) = 0;

			void ReloadLog(const size_t &thread_id){
				FILE *infile_ptr = infiles_[thread_id];
				CommandLogEntries &log_batch = log_entries_[thread_id];
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
						log_batch.push_back(new CommandLogEntry(timestamp, event_tuple));
					}
					file_pos += entry.size_;
				}
				assert(file_pos == file_size);
				entry.Release();
			}

			void ReorderLog(){
				for (size_t i = 0; i < thread_count_; ++i){
					std::cout << "thread id=" << i << ", size=" << log_entries_[i].size() << std::endl;
				}
				CommandLogEntries::iterator *iterators = new CommandLogEntries::iterator[thread_count_];
				for (size_t i = 0; i < thread_count_; ++i){
					iterators[i] = log_entries_[i].begin();
				}
				while (true){
					bool all_finished = true;
					uint64_t min_ts = -1;
					size_t thread_id = SIZE_MAX;
					CommandLogEntries::iterator min_entry;
					for (size_t i = 0; i < thread_count_; ++i){
						if (iterators[i] != log_entries_[i].end()){
							if (min_ts == -1 || (*iterators[i])->timestamp_ < min_ts){
								min_ts = (*iterators[i])->timestamp_;
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
						ordered_log_entries_.push_back(*min_entry);
						++iterators[thread_id];
					}
				}
				delete[] iterators;
				iterators = NULL;
				std::cout << "full log size=" << ordered_log_entries_.size() << std::endl;
			
			}

			void ProcessLog(){
				PrepareProcedures();
				TransactionManager *txn_manager = new TransactionManager(storage_manager_, NULL);
				StoredProcedure **procedures = new StoredProcedure*[registers_.size()];
				for (auto &entry : registers_){
					procedures[entry.first] = entry.second();
					procedures[entry.first]->SetTransactionManager(txn_manager);
				}
				CharArray ret;
				ret.char_ptr_ = new char[1024];
				ExeContext exe_context;
				for (size_t i = 0; i < ordered_log_entries_.size(); ++i){
					TxnParam *param = ordered_log_entries_.at(i)->param_;
					ret.size_ = 0;
					procedures[param->type_]->Execute(param, ret, exe_context);
				}
			}

		private:
			CommandReplayer(const CommandReplayer &);
			CommandReplayer& operator=(const CommandReplayer &);

		protected:
			std::unordered_map<size_t, std::function<StoredProcedure*()>> registers_;

		private:
			CommandLogEntries *log_entries_;
			CommandLogEntries ordered_log_entries_;
		};
	}
}

#endif
