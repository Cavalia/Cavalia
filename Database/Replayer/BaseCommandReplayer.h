#pragma once
#ifndef __CAVALIA_DATABASE_BASE_COMMAND_REPLAYER_H__
#define __CAVALIA_DATABASE_BASE_COMMAND_REPLAYER_H__

#include "../Transaction/TransactionManager.h"
#include "../Transaction/StoredProcedure.h"
#include "BaseReplayer.h"

namespace Cavalia{
	namespace Database{
		class BaseCommandReplayer : public BaseReplayer{
		public:
			BaseCommandReplayer(const std::string &filename, BaseStorageManager *const storage_manager, const size_t &thread_count) : BaseReplayer(filename, storage_manager, thread_count, false){
				log_sequences_ = new std::vector<std::pair<uint64_t, BaseLogEntries*>>[thread_count_];
			}
			virtual ~BaseCommandReplayer(){
				delete[] log_sequences_;
				log_sequences_ = NULL;
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
				std::vector<std::pair<uint64_t, BaseLogEntries*>> &log_sequence = log_sequences_[thread_id];

#if defined(COMPRESSION)
				char *compressed_buffer = new char[kLogBufferSize];
#endif
				char *buffer = new char[kLogBufferSize];

				CharArray entry;
				entry.Allocate(1024);
				int result = 0;
				size_t file_pos = 0;
				while (file_pos < file_size){
					uint64_t epoch;
					result = fread(&epoch, sizeof(epoch), 1, infile_ptr);
					assert(result == 1);
					if (epoch != -1){
						log_sequence.push_back(std::make_pair(epoch, new BaseLogEntries()));
					}
					BaseLogEntries *log_entries = log_sequence.at(log_sequence.size() - 1).second;

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
						size_t param_type;
						memcpy(&param_type, buffer + buffer_offset, sizeof(param_type));
						buffer_offset += sizeof(param_type);
					
						uint64_t timestamp;
						memcpy(&timestamp, buffer + buffer_offset, sizeof(timestamp));
						buffer_offset += sizeof(timestamp);

						if (param_type == kAdhocTxn){
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
							log_entries->push_back(log_entry);
							assert(txn_pos == txn_size);
						}
						else{
							memcpy(&entry.size_, buffer + buffer_offset, sizeof(entry.size_));
							buffer_offset += sizeof(entry.size_);
							memcpy(entry.char_ptr_, buffer + buffer_offset, entry.size_);
							buffer_offset += entry.size_;
							TxnParam* txn_param = DeserializeParam(param_type, entry);
							if (txn_param != NULL){
								log_entries->push_back(new CommandLogEntry(timestamp, txn_param));
							}
						}
					}
					assert(buffer_offset == buffer_size);
				}
				assert(file_pos == file_size);
				entry.Release();

#if defined(COMPRESSION)
				delete[] compressed_buffer;
				compressed_buffer = NULL;
#endif
				delete[] buffer;
				buffer = NULL;
			}

			void ReorderLog(){
				int batch_count = log_sequences_[0].size();
				
				
				for (size_t i = 0; i < thread_count_; ++i){
					std::cout << "thread id=" << i << ", size=" << log_sequences_[i].size() << std::endl;
				}
				BaseLogEntries::iterator *iterators = new BaseLogEntries::iterator[thread_count_];
				
				for (size_t k = 0; k < batch_count; ++k){
					// check epoch.
					uint64_t curr_epoch = log_sequences_[0].at(k).first;
					for (size_t i = 1; i < thread_count_; ++i){
						assert(log_sequences_[i].at(k).first == curr_epoch);
					}
					std::cout << "current epoch=" << curr_epoch << std::endl;
					BaseLogEntries *serial_log_entries = new BaseLogEntries();
					serial_log_sequences_.push_back(std::make_pair(curr_epoch, serial_log_entries));

					for (size_t i = 0; i < thread_count_; ++i){
						iterators[i] = log_sequences_[i].at(k).second->begin();
					}
					while (true){
						uint64_t min_ts = -1;
						size_t thread_id = SIZE_MAX;
						BaseLogEntries::iterator min_entry;
						for (size_t i = 0; i < thread_count_; ++i){
							if (iterators[i] != log_sequences_[i].at(k).second->end()){
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
							serial_log_entries->push_back(*min_entry);
							++iterators[thread_id];
						}
					}
				}
				delete[] iterators;
				iterators = NULL;
				
				size_t total_size = 0;
				for (size_t k = 0; k < batch_count; ++k){
					std::cout << "log size=" << serial_log_sequences_.at(k).second->size() << std::endl;
					total_size += serial_log_sequences_.at(k).second->size();
				}
				std::cout << "full log size=" << total_size << std::endl;
			}

			virtual void ProcessLog() = 0;

		protected:
			virtual TxnParam* DeserializeParam(const size_t &param_type, const CharArray &entry) = 0;
			virtual RecordSchema *GetRecordSchema(const size_t &table_id) = 0;

		private:
			BaseCommandReplayer(const BaseCommandReplayer &);
			BaseCommandReplayer& operator=(const BaseCommandReplayer &);

		protected:
			std::vector<std::pair<uint64_t, BaseLogEntries*>> *log_sequences_;
			std::vector<std::pair<uint64_t, BaseLogEntries*>> serial_log_sequences_;
		};
	}
}

#endif
