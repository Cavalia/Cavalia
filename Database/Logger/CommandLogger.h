#pragma once
#ifndef __CAVALIA_DATABASE_COMMAND_LOGGER_H__
#define __CAVALIA_DATABASE_COMMAND_LOGGER_H__

#include "../Transaction/TxnParam.h"
#include "BaseLogger.h"

namespace Cavalia {
	namespace Database {
		class CommandLogger : public BaseLogger {
		public:
			CommandLogger(const std::string &dir_name, const size_t &thread_count) : BaseLogger(dir_name, thread_count) {
				buffers_ = new CharArray[thread_count_];
				for (size_t i = 0; i < thread_count_; ++i){
					buffers_[i].Allocate(kCommandLogBufferSize);
				}
			}
			virtual ~CommandLogger() {
				for (size_t i = 0; i < thread_count_; ++i){
					buffers_[i].Clear();
				}
				delete[] buffers_;
			}

			void CommitTransaction(const size_t &thread_id, const uint64_t &commit_ts, const size_t &txn_type, TxnParam *param) {
				// write stored procedure type.
				memcpy(buffers_[thread_id].char_ptr_, (char*)(&txn_type), sizeof(txn_type));
				size_t tmp_size = 0;
				// write parameters. get tmp_size first.
				param->Serialize(buffers_[thread_id].char_ptr_ + sizeof(txn_type)+sizeof(tmp_size), tmp_size);
				// write parameter size.
				memcpy(buffers_[thread_id].char_ptr_ + sizeof(txn_type), (char*)(&tmp_size), sizeof(tmp_size));
				
				size_t buffer_offset = sizeof(txn_type)+sizeof(tmp_size)+tmp_size;
				outfiles_[thread_id].write(buffers_[thread_id].char_ptr_, buffer_offset);
				outfiles_[thread_id].flush();
			}

		private:
			CommandLogger(const CommandLogger &);
			CommandLogger& operator=(const CommandLogger &);

		private:
			CharArray *buffers_;
		};
	}
}

#endif
