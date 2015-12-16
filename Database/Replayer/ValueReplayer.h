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

		private:
#if defined(COMPRESSION)
			void ReloadLog(const size_t &thread_id){
				FILE *infile_ptr = infiles_[thread_id];
				fseek(infile_ptr, 0L, SEEK_END);
				size_t file_size = ftell(infile_ptr);
				rewind(infile_ptr);
				ValueLogEntries &log_batch = log_entries_[thread_id];

				// buffer.
				char *compressed_buffer = new char[kLogBufferSize];
				size_t compressed_buffer_size = 0;
				char *buffer = new char[kLogBufferSize];
				size_t buffer_size = 0;
				size_t buffer_offset = 0;

				// context.
				LZ4F_decompressionContext_t ctx;
				LZ4F_errorCode_t err = LZ4F_createDecompressionContext(&ctx, LZ4F_VERSION);
				assert(err == OK_NoError);

				int result = 0;
				size_t file_pos = 0;
				while (file_pos < file_size){
					result = fread(&compressed_buffer_size, sizeof(compressed_buffer_size), 1, infile_ptr);
					assert(result == 1);
					result = fread(compressed_buffer, sizeof(char), compressed_buffer_size, infile_ptr);
					assert(result == compressed_buffer_size);
					file_pos += sizeof(compressed_buffer_size)+compressed_buffer_size;

				}
				assert(file_pos == file_size);

				LZ4F_freeDecompressionContext(ctx);
				delete[] compressed_buffer;
				compressed_buffer = NULL;
				delete[] buffer;
				buffer = NULL;
			}
#else
			void ReloadLog(const size_t &thread_id){
				FILE *infile_ptr = infiles_[thread_id];
				fseek(infile_ptr, 0L, SEEK_END);
				size_t file_size = ftell(infile_ptr);
				rewind(infile_ptr);
				ValueLogEntries &log_batch = log_entries_[thread_id];
				int result = 0;
				size_t file_pos = 0;
				while (file_pos < file_size){
					uint64_t timestamp;
					result = fread(&timestamp, sizeof(timestamp), 1, infile_ptr);
					assert(result == 1);
					ValueLogEntry *log_entry = new ValueLogEntry(timestamp);
					size_t txn_size;
					result = fread(&txn_size, sizeof(txn_size), 1, infile_ptr);
					assert(result == 1);
					// set commit timestamp.
					size_t txn_pos = 0;
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
					file_pos += sizeof(timestamp) + sizeof(txn_size) + txn_size;
				}
				assert(file_pos == file_size);
			}
#endif

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
