#pragma once
#ifndef __CAVALIA_DATABASE_VALUE_REPLAYER_H__
#define __CAVALIA_DATABASE_VALUE_REPLAYER_H__

#include "BaseReplayer.h"

namespace Cavalia{
	namespace Database{

		class ValueReplayer : public BaseReplayer{
		public:
			ValueReplayer(const std::string &filename, BaseStorageManager *const storage_manager, const size_t &thread_count) : BaseReplayer(filename, storage_manager, thread_count, true){
				log_entries_ = new ValueLogEntries[thread_count_];
			}
			virtual ~ValueReplayer(){
				delete[] log_entries_;
				log_entries_ = NULL;
			}

			virtual void Start(){
				TimeMeasurer timer;
				timer.StartTimer();
				boost::thread_group reloaders;
				for (size_t i = 0; i < thread_count_; ++i){
					reloaders.create_thread(boost::bind(&ValueReplayer::ReloadLog, this, i));
				}
				reloaders.join_all();
				timer.EndTimer();
				std::cout << "Reload log elapsed time=" << timer.GetElapsedMilliSeconds() << "ms." << std::endl;
				timer.StartTimer();
				boost::thread_group processors;
				for (size_t i = 0; i < thread_count_; ++i){
					processors.create_thread(boost::bind(&ValueReplayer::ProcessLog, this, i));
				}
				processors.join_all();
				timer.EndTimer();
				std::cout << "Process log elapsed time=" << timer.GetElapsedMilliSeconds() << "ms." << std::endl;
			}

			void ReloadLog(const size_t &thread_id){
				FILE *infile_ptr = infiles_[thread_id];
				fseek(infile_ptr, 0L, SEEK_END);
				size_t file_size = ftell(infile_ptr);
				rewind(infile_ptr);
				ValueLogEntries &log_batch = log_entries_[thread_id];
				size_t file_pos = 0;
				int result = 0;
				while (file_pos < file_size){
					ValueLogEntry *log_entry = new ValueLogEntry(0);
					size_t txn_size;
					result = fread(&txn_size, sizeof(txn_size), 1, infile_ptr);
					assert(result == 1);
					uint64_t timestamp;
					result = fread(&timestamp, sizeof(timestamp), 1, infile_ptr);
					assert(result == 1);
					// set commit timestamp.
					log_entry->timestamp_ = timestamp;
					size_t txn_pos = sizeof(txn_size) + sizeof(timestamp);
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
					file_pos += txn_size;
					log_batch.push_back(log_entry);
				}
				assert(file_pos == file_size);
			}

			void ProcessLog(const size_t &thread_id){
				for (size_t i = 0; i < log_entries_[thread_id].size(); ++i){
					auto *log_entry_ptr = log_entries_[thread_id].at(i);
					uint64_t txn_ts = log_entry_ptr->timestamp_;
					for (size_t k = 0; k < log_entry_ptr->element_count_; ++k){
						ValueLogElement *log_element_ptr = &(log_entry_ptr->elements_[k]);
						if (log_element_ptr->type_ == kInsert){
							SchemaRecord *record_ptr = new SchemaRecord(GetRecordSchema(log_element_ptr->table_id_), log_element_ptr->data_ptr_);
							//storage_manager_->tables_[log_element_ptr->table_id_]->InsertRecord(new TableRecord(record_ptr));
						}
						else if (log_element_ptr->type_ == kUpdate){
							SchemaRecord *record_ptr = new SchemaRecord(GetRecordSchema(log_element_ptr->table_id_), log_element_ptr->data_ptr_);
							TableRecord *tb_record_ptr = NULL;
							storage_manager_->tables_[log_element_ptr->table_id_]->SelectKeyRecord(record_ptr->GetPrimaryKey(), tb_record_ptr);
							tb_record_ptr->content_.AcquireWriteLock();
							if (txn_ts > tb_record_ptr->content_.GetTimestamp()){
								SchemaRecord *tmp_ptr = tb_record_ptr->record_;
								tb_record_ptr->record_ = record_ptr;
								tb_record_ptr->content_.SetTimestamp(txn_ts);
								tb_record_ptr->content_.ReleaseWriteLock();
								delete tmp_ptr;
								tmp_ptr = NULL;
							}
							else{
								tb_record_ptr->content_.ReleaseWriteLock();
								delete record_ptr;
								record_ptr = NULL;
							}
						}
						else if (log_element_ptr->type_ == kDelete){
							//storage_manager_->tables_[log_element_ptr->table_id_]->DeleteRecord();
						}
					}
				}
			}

			virtual RecordSchema *GetRecordSchema(const size_t &table_id) = 0;

		private:
			ValueReplayer(const ValueReplayer &);
			ValueReplayer& operator=(const ValueReplayer &);

		private:
			ValueLogEntries *log_entries_;
		};
	}
}

#endif
