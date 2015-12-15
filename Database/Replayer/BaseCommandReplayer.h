#pragma once
#ifndef __CAVALIA_DATABASE_BASE_COMMAND_REPLAYER_H__
#define __CAVALIA_DATABASE_BASE_COMMAND_REPLAYER_H__

#include <unordered_map>
#include "../Transaction/TransactionManager.h"
#include "../Transaction/StoredProcedure.h"
#include "BaseReplayer.h"

namespace Cavalia{
	namespace Database{
		class BaseCommandReplayer : public BaseReplayer{
		public:
			BaseCommandReplayer(const std::string &filename, BaseStorageManager *const storage_manager, const size_t &thread_count) : BaseReplayer(filename, storage_manager, thread_count, false){
				log_entries_ = new BaseLogEntries[thread_count_];
			}
			virtual ~BaseCommandReplayer(){
				delete[] log_entries_;
				log_entries_ = NULL;
			}

			virtual void Start(){
				TimeMeasurer timer;
				timer.StartTimer();
				boost::thread_group reloaders;
				for (size_t i = 0; i < thread_count_; ++i){
					reloaders.create_thread(boost::bind(&BaseCommandReplayer::ReloadLog, this, i));
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
			void ReloadLog(const size_t &thread_id){
				FILE *infile_ptr = infiles_[thread_id];
				fseek(infile_ptr, 0L, SEEK_END);
				size_t file_size = ftell(infile_ptr);
				rewind(infile_ptr);
				BaseLogEntries &log_batch = log_entries_[thread_id];
				size_t file_pos = 0;
				CharArray entry;
				entry.Allocate(1024);
				int result = 0;
				while (file_pos < file_size){
					size_t param_type;
					result = fread(&param_type, sizeof(param_type), 1, infile_ptr);
					assert(result == 1);
					file_pos += sizeof(param_type);
					if (param_type == kAdHoc){
						ValueLogEntry *log_entry = new ValueLogEntry();
						size_t txn_size;
						result = fread(&txn_size, sizeof(txn_size), 1, infile_ptr);
						assert(result == 1);
						uint64_t timestamp;
						result = fread(&timestamp, sizeof(timestamp), 1, infile_ptr);
						assert(result == 1);
						// set commit timestamp.
						log_entry->timestamp_ = timestamp;
						size_t txn_pos = sizeof(txn_size)+sizeof(timestamp);
						while (txn_pos < txn_size){
							ValueLogElement *log_element = log_entry->NewValueLogElement();
							result = fread(&log_element->type_, sizeof(log_element->type_), 1, infile_ptr);
							assert(result == 1);
							txn_pos += sizeof(log_element->type_);
							result = fread(&log_element->table_id_, sizeof(log_element->table_id_), 1, infile_ptr);
							assert(result == 1);
							txn_pos += sizeof(log_element->table_id_);
							result = fread(&log_element->data_size_, sizeof(log_element->data_size_), 1, infile_ptr);
							assert(result == 1);
							txn_pos += sizeof(log_element->data_size_);
							log_element->data_ptr_ = new char[log_element->data_size_];
							result = fread(log_element->data_ptr_, 1, log_element->data_size_, infile_ptr);
							assert(result == log_element->data_size_);
							txn_pos += log_element->data_size_;
						}
						assert(txn_pos == txn_size);
						log_batch.push_back(log_entry);
						file_pos += txn_size;
					}
					else{
						uint64_t timestamp;
						result = fread(&timestamp, sizeof(timestamp), 1, infile_ptr);
						assert(result == 1);
						file_pos += sizeof(timestamp);
						result = fread(&entry.size_, sizeof(entry.size_), 1, infile_ptr);
						assert(result == 1);
						file_pos += sizeof(entry.size_);
						result = fread(entry.char_ptr_, 1, entry.size_, infile_ptr);
						assert(result == entry.size_);
						TxnParam* txn_param = DeserializeParam(param_type, entry);
						if (txn_param != NULL){
							log_batch.push_back(new CommandLogEntry(timestamp, txn_param));
						}
						file_pos += entry.size_;
					}
				}
				assert(file_pos == file_size);
				entry.Release();
			}

			void ReorderLog(){
				for (size_t i = 0; i < thread_count_; ++i){
					std::cout << "thread id=" << i << ", size=" << log_entries_[i].size() << std::endl;
				}
				BaseLogEntries::iterator *iterators = new BaseLogEntries::iterator[thread_count_];
				for (size_t i = 0; i < thread_count_; ++i){
					iterators[i] = log_entries_[i].begin();
				}
				while (true){
					uint64_t min_ts = -1;
					size_t thread_id = SIZE_MAX;
					BaseLogEntries::iterator min_entry;
					for (size_t i = 0; i < thread_count_; ++i){
						if (iterators[i] != log_entries_[i].end()){
							if (min_ts == -1 || (*iterators[i])->timestamp_ < min_ts){
								min_ts = (*iterators[i])->timestamp_;
								min_entry = iterators[i];
								thread_id = i;
							}
						}
					}
					// all finished
					if (min_ts == -1){
						break;
					}
					else{
						serial_log_entries_.push_back(*min_entry);
						++iterators[thread_id];
					}
				}
				delete[] iterators;
				iterators = NULL;
				std::cout << "full log size=" << serial_log_entries_.size() << std::endl;
			}

			virtual void ProcessLog() = 0;

		protected:
			virtual TxnParam* DeserializeParam(const size_t &param_type, const CharArray &entry) = 0;
			virtual RecordSchema *GetRecordSchema(const size_t &table_id) = 0;

		private:
			BaseCommandReplayer(const BaseCommandReplayer &);
			BaseCommandReplayer& operator=(const BaseCommandReplayer &);

		protected:
			std::unordered_map<size_t, std::function<StoredProcedure*()>> registers_;

		protected:
			BaseLogEntries *log_entries_;
			BaseLogEntries serial_log_entries_;
		};
	}
}

#endif
