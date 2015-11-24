#pragma once
#ifndef __CAVALIA_DATABASE_VALUE_LOG_BUFFER_H__
#define __CAVALIA_DATABASE_vALUE_LOG_BUFFER_H__

#include <cassert>
#include <CharArray.h>
#include "../Meta/MetaTypes.h"

namespace Cavalia {
	namespace Database {
		class ValueLogBuffer {
		public:
			ValueLogBuffer() {
				buffer_.Allocate(kValueLogBufferSize);
				buffer_offset_ = 0;
			}

			~ValueLogBuffer() {
				buffer_.Clear();
			}

			virtual void InsertRecord(const size_t &table_id, char *data, const size_t &data_size) {
				memcpy(buffer_.char_ptr_, (char*)(&kInsert), sizeof(uint8_t));
				buffer_offset_ += sizeof(uint8_t);
				memcpy(buffer_.char_ptr_ + buffer_offset_, (char*)(&table_id), sizeof(size_t));
				buffer_offset_ += sizeof(size_t);
				memcpy(buffer_.char_ptr_ + buffer_offset_, (char*)(&data_size), sizeof(size_t));
				buffer_offset_ += sizeof(size_t);
				memcpy(buffer_.char_ptr_ + buffer_offset_, data, data_size);
				buffer_offset_ += data_size;
			}

			virtual void UpdateRecord(const size_t &table_id, char *data, const size_t &data_size) {
				memcpy(buffer_.char_ptr_, (char*)(&kUpdate), sizeof(uint8_t));
				buffer_offset_ += sizeof(uint8_t);
				memcpy(buffer_.char_ptr_ + buffer_offset_, (char*)(&table_id), sizeof(size_t));
				buffer_offset_ += sizeof(size_t);
				memcpy(buffer_.char_ptr_ + buffer_offset_, (char*)(&data_size), sizeof(size_t));
				buffer_offset_ += sizeof(size_t);
				memcpy(buffer_.char_ptr_ + buffer_offset_, data, data_size);
				buffer_offset_ += data_size;
			}

			virtual void DeleteRecord(const size_t &table_id, const std::string &primary_key) {
				memcpy(buffer_.char_ptr_, (char*)(&kDelete), sizeof(uint8_t));
				buffer_offset_ += sizeof(uint8_t);
				memcpy(buffer_.char_ptr_ + buffer_offset_, (char*)(&table_id), sizeof(size_t));
				buffer_offset_ += sizeof(size_t);
				size_t size = primary_key.size();
				memcpy(buffer_.char_ptr_ + buffer_offset_, (char*)(&size), sizeof(size_t));
				buffer_offset_ += sizeof(size_t);
				memcpy(buffer_.char_ptr_ + buffer_offset_, primary_key.c_str(), size);
				buffer_offset_ += size;
			}

			CharArray* Commit() {
				buffer_offset_ = 0;
				return &buffer_;
			}

		private:
			ValueLogBuffer(const ValueLogBuffer &);
			ValueLogBuffer& operator=(const ValueLogBuffer &);

		private:
			CharArray buffer_;
			size_t buffer_offset_;
		};
	}
}

#endif
