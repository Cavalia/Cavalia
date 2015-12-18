/*
#pragma once
#ifndef __CAVALIA_DATABASE_ACCESS_LOGGER_H__
#define __CAVALIA_DATABASE_ACCESS_LOGGER_H__

#include "BaseLogger.h"

namespace Cavalia{
	namespace Database{
		class AccessLogger : public BaseLogger{
		public:
			AccessLogger(const std::string &dir_name, const size_t &thread_count) : BaseLogger(dir_name, thread_count, true){
				txn_header_size_ = sizeof(size_t);
			}

			virtual ~AccessLogger(){}

			void InsertRecord(const size_t &thread_id, const uint8_t &table_id, char *data, const uint8_t &data_size, const uint64_t &commit_ts) {
				ThreadLogBuffer *buf_struct_ptr = thread_log_buffer_[thread_id];
				char *buffer_ptr = buffers_[thread_id] + buf_struct_ptr->buffer_offset_;
				size_t &offset_ref = buf_struct_ptr->txn_offset_;
				memcpy(buffer_ptr + offset_ref, (char*)(&kInsert), sizeof(uint8_t));
				offset_ref += sizeof(uint8_t);
				memcpy(buffer_ptr + offset_ref, (char*)(&table_id), sizeof(uint8_t));
				offset_ref += sizeof(uint8_t);
				memcpy(buffer_ptr + offset_ref, (char*)(&data_size), sizeof(uint8_t));
				offset_ref += sizeof(uint8_t);
				memcpy(buffer_ptr + offset_ref, data, data_size);
				offset_ref += data_size;
				memcpy(buffer_ptr + offset_ref, (char*)(&commit_ts), sizeof(uint64_t));
				offset_ref += sizeof(uint64_t);
			}

			void UpdateRecord(const size_t &thread_id, const uint8_t &table_id, char *data, const uint8_t &data_size, const uint64_t &commit_ts) {
				ThreadLogBuffer *buf_struct_ptr = thread_log_buffer_[thread_id];
				char *buffer_ptr = buffers_[thread_id] + buf_struct_ptr->buffer_offset_;
				size_t &offset_ref = buf_struct_ptr->txn_offset_;
				memcpy(buffer_ptr + offset_ref, (char*)(&kUpdate), sizeof(uint8_t));
				offset_ref += sizeof(uint8_t);
				memcpy(buffer_ptr + offset_ref, (char*)(&table_id), sizeof(uint8_t));
				offset_ref += sizeof(uint8_t);
				memcpy(buffer_ptr + offset_ref, (char*)(&data_size), sizeof(uint8_t));
				offset_ref += sizeof(uint8_t);
				memcpy(buffer_ptr + offset_ref, data, data_size);
				offset_ref += data_size;
				memcpy(buffer_ptr + offset_ref, (char*)(&commit_ts), sizeof(uint64_t));
				offset_ref += sizeof(uint64_t);
			}

			void DeleteRecord(const size_t &thread_id, const uint8_t &table_id, const std::string &primary_key, const uint64_t &commit_ts) {
				ThreadLogBuffer *buf_struct_ptr = thread_log_buffer_[thread_id];
				char *buffer_ptr = buffers_[thread_id] + buf_struct_ptr->buffer_offset_;
				size_t &offset_ref = buf_struct_ptr->txn_offset_;
				memcpy(buffer_ptr + offset_ref, (char*)(&kDelete), sizeof(uint8_t));
				offset_ref += sizeof(uint8_t);
				memcpy(buffer_ptr + offset_ref, (char*)(&table_id), sizeof(uint8_t));
				offset_ref += sizeof(uint8_t);
				size_t size = primary_key.size();
				memcpy(buffer_ptr + offset_ref, (char*)(&size), sizeof(uint8_t));
				offset_ref += sizeof(uint8_t);
				memcpy(buffer_ptr + offset_ref, primary_key.c_str(), size);
				offset_ref += size;
				memcpy(buffer_ptr + offset_ref, (char*)(&commit_ts), sizeof(uint64_t));
				offset_ref += sizeof(uint64_t);
			}

			void CommitTransaction(const size_t &thread_id, const uint64_t &epoch, const uint64_t &commit_ts){
				ThreadLogBuffer *buf_struct_ptr = thread_log_buffer_[thread_id];
				size_t &offset_ref = buf_struct_ptr->buffer_offset_;
				char *buffer_ptr = buffers_[thread_id] + offset_ref;
				memcpy(buffer_ptr, (char*)(&(buf_struct_ptr->txn_offset_)), sizeof(size_t));
				offset_ref += buf_struct_ptr->txn_offset_;
				assert(offset_ref < kLogBufferSize);
				if (epoch != buf_struct_ptr->last_epoch_){
					FILE *file_ptr = outfiles_[thread_id];
					buf_struct_ptr->last_epoch_ = epoch;
#if defined(COMPRESSION)
					size_t& offset = buf_struct_ptr->compressed_buf_offset_;
					size_t n = LZ4F_compressUpdate(buf_struct_ptr->compression_context_, compressed_buffers_[thread_id] + offset, compressed_buf_size_ - offset, buffers_[thread_id], offset_ref, NULL);
					assert(LZ4F_isError(n) == false);
					offset += n;

					// after compression, write into file
					fwrite(compressed_buffers_[thread_id], sizeof(char), offset, file_ptr);
					offset = 0;
#else
					fwrite(buffers_[thread_id], sizeof(char), offset_ref, file_ptr);
#endif
					int ret;
					ret = fflush(file_ptr);
					assert(ret == 0);
#if defined(__linux__)
					ret = fsync(fileno(file_ptr));
					assert(ret == 0);
#endif
					offset_ref = 0;
				}
				buf_struct_ptr->txn_offset_ = txn_header_size_;
			}

		private:
			AccessLogger(const AccessLogger &);
			AccessLogger& operator=(const AccessLogger &);
		};
	}
}
#endif
*/
