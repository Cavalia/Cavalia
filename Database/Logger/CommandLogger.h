#pragma once
#ifndef __CAVALIA_DATABASE_COMMAND_LOGGER_H__
#define __CAVALIA_DATABASE_COMMAND_LOGGER_H__

#include "../Transaction/TxnParam.h"
#include "BaseLogger.h"

namespace Cavalia {
	namespace Database {
		class CommandLogger : public BaseLogger {
		public:
			CommandLogger(const std::string &dir_name, const size_t &thread_count) : BaseLogger(dir_name, thread_count, false) {
				buffers_ = new char*[thread_count_];
				buffer_offsets_ = new size_t[thread_count_];
				for (size_t i = 0; i < thread_count_; ++i){
					buffers_[i] = new char[kCommandLogBufferSize];
					buffer_offsets_[i] = 0;
				}
				last_timestamps_ = new uint64_t[thread_count_];
				for (size_t i = 0; i < thread_count_; ++i){
					last_timestamps_[i] = 0;
				}
			}
			virtual ~CommandLogger() {
				for (size_t i = 0; i < thread_count_; ++i){
					delete[] buffers_[i];
					buffers_[i] = NULL;
				}
				delete[] buffers_;
				buffers_ = NULL;
				delete[] buffer_offsets_;
				buffer_offsets_ = NULL;
				delete[] last_timestamps_;
				last_timestamps_ = NULL;
			}

			void CommitTransaction(const size_t &thread_id, const uint64_t &global_ts, const size_t &txn_type, TxnParam *param) {
				char *buffer_ptr = buffers_[thread_id];
				size_t &offset_ref = buffer_offsets_[thread_id];
				// write stored procedure type.
				memcpy(buffer_ptr + offset_ref, (char*)(&txn_type), sizeof(txn_type));
				size_t tmp_size = 0;
				// write parameters. get tmp_size first.
				param->Serialize(buffer_ptr + offset_ref + sizeof(txn_type)+sizeof(tmp_size), tmp_size);
				// write parameter size.
				memcpy(buffer_ptr + offset_ref + sizeof(txn_type), (char*)(&tmp_size), sizeof(tmp_size));

				offset_ref += sizeof(txn_type)+sizeof(tmp_size)+tmp_size;
				if (global_ts != last_timestamps_[thread_id]){
					FILE *file_ptr = outfiles_[thread_id];
					last_timestamps_[thread_id] = global_ts;
					fwrite(buffer_ptr, sizeof(char), offset_ref, file_ptr);
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

		private:
			char **buffers_;
			size_t *buffer_offsets_;
			uint64_t *last_timestamps_;
		};
	}
}

#endif
