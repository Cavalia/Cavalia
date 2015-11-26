#pragma once
#ifndef __CAVALIA_DATABASE_VALUE_LOGGER_H__
#define __CAVALIA_DATABASE_VALUE_LOGGER_H__

#include <CharArray.h>
#include "BaseLogger.h"
#include "../Meta/MetaTypes.h"

namespace Cavalia{
	namespace Database{
		const uint8_t kInsert = 0;
		const uint8_t kUpdate = 1;
		const uint8_t kDelete = 2;

		class ValueLogger : public BaseLogger{
		public:
			ValueLogger(const std::string &dir_name, const size_t &thread_count) : BaseLogger(dir_name, thread_count){
				buffers_ = new CharArray[thread_count_];
				buffer_offsets_ = new size_t[thread_count_];
				for (size_t i = 0; i < thread_count_; ++i){
					buffers_[i].Allocate(kValueLogBufferSize);
					buffer_offsets_[i] = 0;
				}
			}
			virtual ~ValueLogger(){
				for (size_t i = 0; i < thread_count_; ++i){
					buffers_[i].Clear();
				}
				delete[] buffers_;
				delete[] buffer_offsets_;
			}

			virtual void InsertRecord(const size_t &thread_id, const size_t &table_id, char *data, const size_t &data_size) {
				CharArray &buffer_ref = buffers_[thread_id];
				size_t &offset_ref = buffer_offsets_[thread_id];
				memcpy(buffer_ref.char_ptr_, (char*)(&kInsert), sizeof(uint8_t));
				offset_ref += sizeof(uint8_t);
				memcpy(buffer_ref.char_ptr_ + offset_ref, (char*)(&table_id), sizeof(size_t));
				offset_ref += sizeof(size_t);
				memcpy(buffer_ref.char_ptr_ + offset_ref, (char*)(&data_size), sizeof(size_t));
				offset_ref += sizeof(size_t);
				memcpy(buffer_ref.char_ptr_ + offset_ref, data, data_size);
				offset_ref += data_size;
			}

			virtual void UpdateRecord(const size_t &thread_id, const size_t &table_id, char *data, const size_t &data_size) {
				CharArray &buffer_ref = buffers_[thread_id];
				size_t &offset_ref = buffer_offsets_[thread_id];
				memcpy(buffer_ref.char_ptr_, (char*)(&kUpdate), sizeof(uint8_t));
				offset_ref += sizeof(uint8_t);
				memcpy(buffer_ref.char_ptr_ + offset_ref, (char*)(&table_id), sizeof(size_t));
				offset_ref += sizeof(size_t);
				memcpy(buffer_ref.char_ptr_ + offset_ref, (char*)(&data_size), sizeof(size_t));
				offset_ref += sizeof(size_t);
				memcpy(buffer_ref.char_ptr_ + offset_ref, data, data_size);
				offset_ref += data_size;
			}

			virtual void DeleteRecord(const size_t &thread_id, const size_t &table_id, const std::string &primary_key) {
				CharArray &buffer_ref = buffers_[thread_id];
				size_t &offset_ref = buffer_offsets_[thread_id];
				memcpy(buffer_ref.char_ptr_, (char*)(&kDelete), sizeof(uint8_t));
				offset_ref += sizeof(uint8_t);
				memcpy(buffer_ref.char_ptr_ + offset_ref, (char*)(&table_id), sizeof(size_t));
				offset_ref += sizeof(size_t);
				size_t size = primary_key.size();
				memcpy(buffer_ref.char_ptr_ + offset_ref, (char*)(&size), sizeof(size_t));
				offset_ref += sizeof(size_t);
				memcpy(buffer_ref.char_ptr_ + offset_ref, primary_key.c_str(), size);
				offset_ref += size;
			}

			virtual void CommitTransaction(const size_t &thread_id, const uint64_t &commit_ts){
				outfiles_[thread_id].write(reinterpret_cast<char*>(&buffer_offsets_[thread_id]), sizeof(buffer_offsets_[thread_id]));
				outfiles_[thread_id].write(buffers_[thread_id].char_ptr_, buffer_offsets_[thread_id]);
				outfiles_[thread_id].flush();
				buffer_offsets_[thread_id] = 0;
			}

			virtual void AbortTransaction(const size_t &thread_id){
				buffer_offsets_[thread_id] = 0;
			}

		private:
			ValueLogger(const ValueLogger &);
			ValueLogger& operator=(const ValueLogger &);

		private:
			CharArray *buffers_;
			size_t *buffer_offsets_;
		};
	}
}

#endif
