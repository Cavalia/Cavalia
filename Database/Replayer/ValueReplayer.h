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
			void ReloadLog(const size_t &thread_id){
				FILE *infile_ptr = infiles_[thread_id];
				fseek(infile_ptr, 0L, SEEK_END);
				size_t file_size = ftell(infile_ptr);
				rewind(infile_ptr);
				ValueLogEntries &log_batch = log_entries_[thread_id];

#if defined(COMPRESSION)
				char *compressed_buffer = new char[kLogBufferSize];
#endif
				char *buffer = new char[kLogBufferSize];

				int result = 0;
				size_t file_pos = 0;
				while (file_pos < file_size){
					uint64_t epoch;
					result = fread(&epoch, sizeof(epoch), 1, infile_ptr);
					assert(result == 1);
					size_t log_chunk_size = 0;
					result = fread(&log_chunk_size, sizeof(log_chunk_size), 1, infile_ptr);
					assert(result == 1);

#if defined(COMPRESSION)
					result = fread(compressed_buffer, sizeof(char), log_chunk_size, infile_ptr);
					assert(result == log_chunk_size);
					file_pos += sizeof(epoch)+sizeof(log_chunk_size)+log_chunk_size;

					size_t buffer_size = kLogBufferSize;
					size_t compressed_buffer_size = kLogBufferSize;
					// context.
					LZ4F_decompressionContext_t ctx;
					LZ4F_errorCode_t err = LZ4F_createDecompressionContext(&ctx, LZ4F_VERSION);
					size_t ret = LZ4F_decompress(ctx, buffer, &buffer_size, compressed_buffer, &compressed_buffer_size, NULL);
					assert(LZ4F_isError(ret) == false);
					LZ4F_freeDecompressionContext(ctx);
					assert(compressed_buffer_size == log_chunk_size);
#else
					size_t buffer_size = log_chunk_size;
					result = fread(buffer, sizeof(char), log_chunk_size, infile_ptr);
					assert(result == log_chunk_size);
					file_pos += sizeof(epoch)+sizeof(log_chunk_size)+log_chunk_size;
#endif

					size_t buffer_offset = 0;
					while (buffer_offset < buffer_size) {
						uint64_t timestamp;
						memcpy(&timestamp, buffer + buffer_offset, sizeof(timestamp));
						buffer_offset += sizeof(timestamp);

						ValueLogEntry *log_entry = new ValueLogEntry(timestamp);
						size_t txn_size;
						memcpy(&txn_size, buffer + buffer_offset, sizeof(txn_size));
						buffer_offset += sizeof(txn_size);

						size_t txn_pos = 0;
						while (txn_pos < txn_size){
							ValueLogElement *log_element = log_entry->NewValueLogElement();
							memcpy(&log_element->type_, buffer + buffer_offset, sizeof(log_element->type_));
							buffer_offset += sizeof(log_element->type_);
							txn_pos += sizeof(log_element->type_);

							memcpy(&log_element->table_id_, buffer + buffer_offset, sizeof(log_element->table_id_));
							buffer_offset += sizeof(log_element->table_id_);
							txn_pos += sizeof(log_element->table_id_);

							memcpy(&log_element->data_size_, buffer + buffer_offset, sizeof(log_element->data_size_));
							buffer_offset += sizeof(log_element->data_size_);
							txn_pos += sizeof(log_element->data_size_);

							log_element->data_ptr_ = new char[log_element->data_size_];
							memcpy(log_element->data_ptr_, buffer + buffer_offset, log_element->data_size_);
							buffer_offset += log_element->data_size_;
							txn_pos += log_element->data_size_;
						}
						log_batch.push_back(log_entry);
						assert(txn_pos == txn_size);
					}
					assert(buffer_offset == buffer_size);
				}
				assert(file_pos == file_size);

#if defined(COMPRESSION)
				delete[] compressed_buffer;
				compressed_buffer = NULL;
#endif
				delete[] buffer;
				buffer = NULL;
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
