#pragma once
#ifndef __CAVALIA_DATABASE_COMMAND_LOGGER_H__
#define __CAVALIA_DATABASE_COMMAND_LOGGER_H__

#include "BaseLogger.h"

namespace Cavalia {
	namespace Database {
		class CommandLogger : public BaseLogger {
		public:
			CommandLogger(const std::string &dir_name, const size_t &thread_count) : BaseLogger(dir_name, thread_count, false) {}

			virtual ~CommandLogger() {}

			virtual void CommitTransaction(const size_t &thread_id, const uint64_t &epoch, const uint64_t &commit_ts){}

			virtual void CommitTransaction(const size_t &thread_id, const uint64_t &epoch, const uint64_t &commit_ts, const size_t &txn_type, TxnParam *param) {
				ThreadBufferStruct *buf_struct_ptr = thread_buf_structs_[thread_id];
				char *buffer_ptr = buffers_[thread_id];
				size_t &offset_ref = buf_struct_ptr->buffer_offset_;
				// write stored procedure type.
				memcpy(buffer_ptr + offset_ref, (char*)(&txn_type), sizeof(txn_type));
				offset_ref += sizeof(txn_type);
				// write timestamp.
				memcpy(buffer_ptr + offset_ref, (char*)(&commit_ts), sizeof(commit_ts));
				offset_ref += sizeof(commit_ts);
				size_t tmp_size = 0;
				// write parameters. get tmp_size first.
				param->Serialize(buffer_ptr + offset_ref + sizeof(tmp_size), tmp_size);
				// write parameter size.
				memcpy(buffer_ptr + offset_ref, (char*)(&tmp_size), sizeof(tmp_size));
				offset_ref += sizeof(tmp_size)+tmp_size;

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
					fwrite(buffer_ptr, sizeof(char), offset_ref, file_ptr);
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
			}

		private:
			CommandLogger(const CommandLogger &);
			CommandLogger& operator=(const CommandLogger &);
		};
	}
}

#endif
