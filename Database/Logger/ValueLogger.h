#pragma once
#ifndef __CAVALIA_DATABASE_VALUE_LOGGER_H__
#define __CAVALIA_DATABASE_VALUE_LOGGER_H__

#include "BaseLogger.h"

namespace Cavalia{
	namespace Database{
		class ValueLogger : public BaseLogger{
		public:
			ValueLogger(const std::string &dir_name, const size_t &thread_count) : BaseLogger(dir_name, thread_count, true){
				txn_header_size_ = sizeof(size_t)+sizeof(uint64_t);
			}

			virtual ~ValueLogger(){}

			// | timestamp | param_size | param_content |
			virtual void CommitTransaction(const size_t &thread_id, const uint64_t &epoch, const uint64_t &commit_ts){
				ThreadBufferStruct *buf_struct_ptr = thread_buf_structs_[thread_id];
				size_t &buffer_offset_ref = buf_struct_ptr->buffer_offset_;
				char *curr_buffer_ptr = buf_struct_ptr->buffer_ptr_ + buffer_offset_ref;
				memcpy(curr_buffer_ptr, (char*)(&commit_ts), sizeof(uint64_t));
				size_t txn_size = buf_struct_ptr->txn_offset_ - txn_header_size_;
				memcpy(curr_buffer_ptr + sizeof(uint64_t), (char*)(&txn_size), sizeof(size_t));
				buffer_offset_ref += buf_struct_ptr->txn_offset_;
				assert(buffer_offset_ref < kLogBufferSize);

				if (epoch != buf_struct_ptr->last_epoch_){
					FILE *file_ptr = outfiles_[thread_id];
					buf_struct_ptr->last_epoch_ = epoch;
#if defined(COMPRESSION)
					char *compressed_buffer_ptr = buf_struct_ptr->compressed_buffer_ptr_;
					size_t bound = LZ4F_compressFrameBound(buffer_offset_ref, NULL);
					size_t n = LZ4F_compressFrame(compressed_buffer_ptr, bound, buf_struct_ptr->buffer_ptr_, buffer_offset_ref, NULL);
					assert(LZ4F_isError(n) == false);

					// after compression, write into file
					fwrite(compressed_buffer_ptr, sizeof(char), n, file_ptr);
#else
					fwrite(buf_struct_ptr->buffer_ptr_, sizeof(char), buffer_offset_ref, file_ptr);
#endif
					buffer_offset_ref = 0;
					int ret;
					ret = fflush(file_ptr);
					assert(ret == 0);
#if defined(__linux__)
					ret = fsync(fileno(file_ptr));
					assert(ret == 0);
#endif
				}
				buf_struct_ptr->txn_offset_ = txn_header_size_;
			}


			/*virtual void CommitTransaction(const size_t &thread_id, const uint64_t &epoch, const uint64_t &commit_ts, const size_t &txn_type, TxnParam *param){
				assert(false);
			}*/

		private:
			ValueLogger(const ValueLogger &);
			ValueLogger& operator=(const ValueLogger &);
		};
	}
}

#endif
