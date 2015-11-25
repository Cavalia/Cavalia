#pragma once
#ifndef __CAVALIA_DATABASE_COMMAND_LOG_BUFFER_H__
#define __CAVALIA_DATABASE_COMMAND_LOG_BUFFER_H__

#include <cassert>
#include <CharArray.h>
#include "../Meta/MetaTypes.h"

namespace Cavalia {
	namespace Database {
		class CommandLogBuffer {
		public:
			CommandLogBuffer() {
				buffer_.Allocate(kCommandLogBufferSize);
				buffer_offset_ = 0;
			}

			~CommandLogBuffer() {
				buffer_.Clear();
			}

			CharArray* Commit(const size_t &txn_type, TxnParam *param) {
				memcpy(buffer_.char_ptr_, (char*)(&txn_type), sizeof(txn_type));
				size_t tmp_size = 0;
				param->Serialize(buffer_.char_ptr_ + sizeof(txn_type) + sizeof(tmp_size), tmp_size);
				memcpy(buffer_.char_ptr_ + sizeof(txn_type), (char*)(&tmp_size), sizeof(tmp_size));
				buffer_offset_ = sizeof(txn_type) + sizeof(tmp_size) + tmp_size;
				buffer_offset_ = 0;
				return &buffer_;
			}

		private:
			CommandLogBuffer(const CommandLogBuffer &);
			CommandLogBuffer& operator=(const CommandLogBuffer &);

		private:
			CharArray buffer_;
			size_t buffer_offset_;
		};
	}
}

#endif
