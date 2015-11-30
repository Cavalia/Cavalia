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
				buffers_ = new CharArray[thread_count_];
				for (size_t i = 0; i < thread_count_; ++i){
					buffers_[i].Allocate(kCommandLogBufferSize);
				}
				last_timestamps_ = new uint64_t[thread_count_];
				counts_ = new size_t[thread_count_];
				for (size_t i = 0; i < thread_count_; ++i){
					last_timestamps_[i] = 0;
					counts_[i] = 0;
				}
			}
			virtual ~CommandLogger() {
				for (size_t i = 0; i < thread_count_; ++i){
					buffers_[i].Clear();
				}
				delete[] buffers_;
				buffers_ = NULL;
				delete[] last_timestamps_;
				last_timestamps_ = NULL;
				delete[] counts_;
				counts_ = NULL;
			}

			void CommitTransaction(const size_t &thread_id, const uint64_t &global_ts, const size_t &txn_type, TxnParam *param) {
				++counts_[thread_id];
				if (global_ts != last_timestamps_[thread_id]){
					last_timestamps_[thread_id] = global_ts;
					std::cout << "count=" << counts_[thread_id] << std::endl;
					counts_[thread_id] = 0;
				}
				else{
					// write stored procedure type.
					memcpy(buffers_[thread_id].char_ptr_, (char*)(&txn_type), sizeof(txn_type));
					size_t tmp_size = 0;
					// write parameters. get tmp_size first.
					param->Serialize(buffers_[thread_id].char_ptr_ + sizeof(txn_type)+sizeof(tmp_size), tmp_size);
					// write parameter size.
					memcpy(buffers_[thread_id].char_ptr_ + sizeof(txn_type), (char*)(&tmp_size), sizeof(tmp_size));

					size_t buffer_offset = sizeof(txn_type)+sizeof(tmp_size)+tmp_size;
					fwrite(buffers_[thread_id].char_ptr_, sizeof(char), buffer_offset, outfiles_[thread_id]);
					int ret;
					ret = fflush(outfiles_[thread_id]);
					assert(ret == 0);
#if defined(__linux__)
					ret = fsync(fileno(outfiles_[thread_id]));
					assert(ret == 0);
#endif
				}
			}

		private:
			CommandLogger(const CommandLogger &);
			CommandLogger& operator=(const CommandLogger &);

		private:
			CharArray *buffers_;
			uint64_t *last_timestamps_;
			size_t *counts_;
		};
	}
}

#endif
