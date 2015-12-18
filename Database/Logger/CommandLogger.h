#pragma once
#ifndef __CAVALIA_DATABASE_COMMAND_LOGGER_H__
#define __CAVALIA_DATABASE_COMMAND_LOGGER_H__

#include "BaseLogger.h"

namespace Cavalia {
	namespace Database {
		class CommandLogger : public BaseLogger {
		public:
			CommandLogger(const std::string &dir_name, const size_t &thread_count) : BaseLogger(dir_name, thread_count, false){}
			virtual ~CommandLogger() {}

			// commit value logging.
			// | param_type | timestamp | param_size | param_content |
			virtual void CommitTransaction(const size_t &thread_id, const uint64_t &epoch, const uint64_t &commit_ts, AccessList<kMaxAccessNum> &access_list){
				ThreadLogBuffer *tlb_ptr = thread_log_buffer_[thread_id];
				size_t &buffer_offset_ref = tlb_ptr->buffer_offset_;
				if (tlb_ptr->last_epoch_ == -1){
					tlb_ptr->last_epoch_ = epoch;
				}
				else if (tlb_ptr->last_epoch_ != epoch){
					assert(tlb_ptr->last_epoch_ + 1 == epoch);
					FILE *file_ptr = outfiles_[thread_id];
					int result;
					// record epoch.
					result = fwrite(&tlb_ptr->last_epoch_, sizeof(uint64_t), 1, file_ptr);
					assert(result == 1);
					tlb_ptr->last_epoch_ = epoch;
#if defined(COMPRESSION)
					char *compressed_buffer_ptr = tlb_ptr->compressed_buffer_ptr_;
					size_t bound = LZ4F_compressFrameBound(buffer_offset_ref, NULL);
					size_t n = LZ4F_compressFrame(compressed_buffer_ptr, bound, tlb_ptr->buffer_ptr_, buffer_offset_ref, NULL);
					assert(LZ4F_isError(n) == false);

					// after compression, write into file
					result = fwrite(&n, sizeof(size_t), 1, file_ptr);
					assert(result == 1);
					result = fwrite(compressed_buffer_ptr, sizeof(char), n, file_ptr);
					assert(result == n);
#else
					result = fwrite(&buffer_offset_ref, sizeof(size_t), 1, file_ptr);
					assert(result == 1);
					result = fwrite(tlb_ptr->buffer_ptr_, sizeof(char), buffer_offset_ref, file_ptr);
					assert(result == buffer_offset_ref);
#endif
					buffer_offset_ref = 0;
					result = fflush(file_ptr);
					assert(result == 0);
#if defined(__linux__)
					result = fsync(fileno(file_ptr));
					assert(result == 0);
#endif
				}
				char *curr_buffer_ptr = tlb_ptr->buffer_ptr_ + buffer_offset_ref;
				memcpy(curr_buffer_ptr, (char*)(&kAdhocTxn), sizeof(size_t));
				memcpy(curr_buffer_ptr + sizeof(size_t), (char*)(&commit_ts), sizeof(uint64_t));
				size_t txn_offset = sizeof(size_t)+sizeof(uint64_t)+sizeof(size_t);
				for (size_t i = 0; i < access_list.access_count_; ++i){
					Access *access_ptr = access_list.GetAccess(i);
					if (access_ptr->access_type_ == READ_WRITE){
						SchemaRecord *local_record_ptr = access_ptr->local_record_;
						size_t record_size = local_record_ptr->schema_ptr_->GetSchemaSize();
						memcpy(curr_buffer_ptr + txn_offset, (char*)(&kUpdate), sizeof(uint8_t));
						memcpy(curr_buffer_ptr + txn_offset + sizeof(uint8_t), (char*)(&access_ptr->table_id_), sizeof(size_t));
						memcpy(curr_buffer_ptr + txn_offset + sizeof(uint8_t)+sizeof(size_t), (char*)(&record_size), sizeof(size_t));
						memcpy(curr_buffer_ptr + txn_offset + sizeof(uint8_t)+sizeof(size_t)+sizeof(size_t), local_record_ptr->data_ptr_, record_size);
						txn_offset += sizeof(uint8_t)+sizeof(size_t)+sizeof(size_t)+record_size;
					}
					else if (access_ptr->access_type_ == INSERT_ONLY){
						SchemaRecord *global_record_ptr = access_ptr->access_record_->record_;
						size_t record_size = global_record_ptr->schema_ptr_->GetSchemaSize();
						memcpy(curr_buffer_ptr + txn_offset, (char*)(&kInsert), sizeof(uint8_t));
						memcpy(curr_buffer_ptr + txn_offset + sizeof(uint8_t), (char*)(&access_ptr->table_id_), sizeof(size_t));
						memcpy(curr_buffer_ptr + txn_offset + sizeof(uint8_t)+sizeof(size_t), (char*)(&record_size), sizeof(size_t));
						memcpy(curr_buffer_ptr + txn_offset + sizeof(uint8_t)+sizeof(size_t)+sizeof(size_t), global_record_ptr->data_ptr_, record_size);
						txn_offset += sizeof(uint8_t)+sizeof(size_t)+sizeof(size_t)+record_size;
					}
					else if (access_ptr->access_type_ == DELETE_ONLY){
						SchemaRecord *local_record_ptr = access_ptr->local_record_;
						std::string primary_key = local_record_ptr->GetPrimaryKey();
						size_t key_size = primary_key.size();
						memcpy(curr_buffer_ptr + txn_offset, (char*)(&kDelete), sizeof(uint8_t));
						memcpy(curr_buffer_ptr + txn_offset + sizeof(uint8_t), (char*)(&access_ptr->table_id_), sizeof(size_t));
						memcpy(curr_buffer_ptr + txn_offset + sizeof(uint8_t)+sizeof(size_t), (char*)(&key_size), sizeof(size_t));
						memcpy(curr_buffer_ptr + sizeof(uint8_t)+sizeof(size_t)+sizeof(size_t), primary_key.c_str(), key_size);
						txn_offset += sizeof(uint8_t)+sizeof(size_t)+sizeof(size_t)+key_size;
					}
				}
				size_t txn_size = txn_offset - sizeof(size_t)-sizeof(uint64_t)-sizeof(size_t);
				memcpy(curr_buffer_ptr + sizeof(size_t)+sizeof(uint64_t), (char*)(&txn_offset), sizeof(size_t));
				buffer_offset_ref += sizeof(size_t)+sizeof(uint64_t)+sizeof(size_t)+txn_offset;
				assert(buffer_offset_ref < kLogBufferSize);
			}

			// commit command logging.
			// | param_type | timestamp | param_size | param_content |
			virtual void CommitTransaction(const size_t &thread_id, const uint64_t &epoch, const uint64_t &commit_ts, const size_t &txn_type, TxnParam *param){
				ThreadLogBuffer *tlb_ptr = thread_log_buffer_[thread_id];
				size_t &buffer_offset_ref = tlb_ptr->buffer_offset_;
				if (tlb_ptr->last_epoch_ == -1){
					tlb_ptr->last_epoch_ = epoch;
				}
				else if (tlb_ptr->last_epoch_ != epoch){
					assert(tlb_ptr->last_epoch_ + 1 == epoch);
					FILE *file_ptr = outfiles_[thread_id];
					int result;
					// record epoch.
					result = fwrite(&tlb_ptr->last_epoch_, sizeof(uint64_t), 1, file_ptr);
					assert(result == 1);
					tlb_ptr->last_epoch_ = epoch;
#if defined(COMPRESSION)
					char *compressed_buffer_ptr = tlb_ptr->compressed_buffer_ptr_;
					size_t bound = LZ4F_compressFrameBound(buffer_offset_ref, NULL);
					size_t n = LZ4F_compressFrame(compressed_buffer_ptr, bound, tlb_ptr->buffer_ptr_, buffer_offset_ref, NULL);
					assert(LZ4F_isError(n) == false);

					// after compression, write into file
					result = fwrite(&n, sizeof(size_t), 1, file_ptr);
					assert(result == 1);
					result = fwrite(compressed_buffer_ptr, sizeof(char), n, file_ptr);
					assert(result == n);
#else
					result = fwrite(&buffer_offset_ref, sizeof(size_t), 1, file_ptr);
					assert(result == 1);
					result = fwrite(tlb_ptr->buffer_ptr_, sizeof(char), buffer_offset_ref, file_ptr);
					assert(result == buffer_offset_ref);
#endif
					buffer_offset_ref = 0;
					result = fflush(file_ptr);
					assert(result == 0);
#if defined(__linux__)
					result = fsync(fileno(file_ptr));
					assert(result == 0);
#endif
				}
				char *curr_buffer_ptr = tlb_ptr->buffer_ptr_ + buffer_offset_ref;
				// write stored procedure type.
				memcpy(curr_buffer_ptr, (char*)(&txn_type), sizeof(size_t));
				// write timestamp.
				memcpy(curr_buffer_ptr + sizeof(size_t), (char*)(&commit_ts), sizeof(uint64_t));
				size_t tmp_size = 0;
				// write parameters. get tmp_size first.
				param->Serialize(curr_buffer_ptr + sizeof(size_t)+sizeof(uint64_t)+sizeof(size_t), tmp_size);
				// write parameter size.
				memcpy(curr_buffer_ptr + sizeof(size_t)+sizeof(uint64_t), (char*)(&tmp_size), sizeof(size_t));
				buffer_offset_ref += sizeof(size_t)+sizeof(uint64_t)+sizeof(size_t)+tmp_size;
				assert(buffer_offset_ref < kLogBufferSize);
			}

		private:
			CommandLogger(const CommandLogger &);
			CommandLogger& operator=(const CommandLogger &);
		};
	}
}

#endif
