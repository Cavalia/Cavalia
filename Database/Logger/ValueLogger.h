#pragma once
#ifndef __CAVALIA_DATABASE_VALUE_LOGGER_H__
#define __CAVALIA_DATABASE_VALUE_LOGGER_H__

#include "BaseLogger.h"

namespace Cavalia{
	namespace Database{
		class ValueLogger : public BaseLogger{
		public:
			ValueLogger(const std::string &dir_name, const size_t &thread_count) : BaseLogger(dir_name, thread_count, true){}

			virtual ~ValueLogger(){}

			virtual void CommitTransaction(const size_t &thread_id, const uint64_t &epoch, const uint64_t &commit_ts){
				ThreadBufferStruct *buf_struct_ptr = thread_buf_structs_[thread_id];
				char *buffer_ptr = buffers_[thread_id] + buf_struct_ptr->buffer_offset_;
				memcpy(buffer_ptr, (char*)(&(buf_struct_ptr->txn_offset_)), sizeof(size_t));
				memcpy(buffer_ptr + sizeof(size_t), (char*)(&commit_ts), sizeof(uint64_t));
				buf_struct_ptr->buffer_offset_ += buf_struct_ptr->txn_offset_;
				assert(buf_struct_ptr->buffer_offset_ < kLogBufferSize);
				if (epoch != buf_struct_ptr->last_epoch_){
					FILE *file_ptr = outfiles_[thread_id];
					buf_struct_ptr->last_epoch_ = epoch;
#if defined(COMPRESSION)
					size_t& offset = buf_struct_ptr->compressed_buf_offset_;
					size_t n = LZ4F_compressUpdate(buf_struct_ptr->compression_context_, compressed_buffers_[thread_id] + offset, compressed_buf_size_ - offset, buffers_[thread_id], buf_struct_ptr->buffer_offset_, NULL);
					assert(LZ4F_isError(n) == false);
					offset += n;

					// after compression, write into file
					fwrite(compressed_buffers_[thread_id], sizeof(char), offset, file_ptr);
					offset = 0;
#else
					fwrite(buffers_[thread_id], sizeof(char), buf_struct_ptr->buffer_offset_, file_ptr);
#endif
					int ret;
					ret = fflush(file_ptr);
					assert(ret == 0);
#if defined(__linux__)
					ret = fsync(fileno(file_ptr));
					assert(ret == 0);
#endif
					buf_struct_ptr->buffer_offset_ = 0;
				}
				buf_struct_ptr->txn_offset_ = sizeof(size_t)+sizeof(uint64_t);
			}

			virtual void CommitTransaction(const size_t &thread_id, const uint64_t &epoch, const uint64_t &commit_ts, const size_t &txn_type, TxnParam *param){}

		private:
			ValueLogger(const ValueLogger &);
			ValueLogger& operator=(const ValueLogger &);
		};
	}
}

#endif
