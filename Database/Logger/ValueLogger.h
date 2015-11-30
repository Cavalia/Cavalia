#pragma once
#ifndef __CAVALIA_DATABASE_VALUE_LOGGER_H__
#define __CAVALIA_DATABASE_VALUE_LOGGER_H__

#include "../Meta/MetaConstants.h"
#include "BaseLogger.h"

namespace Cavalia{
	namespace Database{

		class ValueLogger : public BaseLogger{
		public:
			ValueLogger(const std::string &dir_name, const size_t &thread_count) : BaseLogger(dir_name, thread_count, true){
				buffers_ = new char*[thread_count_];
				buffer_offsets_ = new size_t[thread_count_];
				txn_offsets_ = new size_t[thread_count_];
				for (size_t i = 0; i < thread_count_; ++i){
					buffers_[i] = new char[kValueLogBufferSize];
					buffer_offsets_[i] = 0;
					txn_offsets_[i] = sizeof(size_t);
				}
				last_timestamps_ = new uint64_t[thread_count_];
				for (size_t i = 0; i < thread_count_; ++i){
					last_timestamps_[i] = 0;
				}
			}
			virtual ~ValueLogger(){
				for (size_t i = 0; i < thread_count_; ++i){
					delete[] buffers_[i];
					buffers_[i] = NULL;
				}
				delete[] buffers_;
				buffers_ = NULL;
				delete[] buffer_offsets_;
				buffer_offsets_ = NULL;
				delete[] txn_offsets_;
				txn_offsets_ = NULL;
				delete[] last_timestamps_;
				last_timestamps_ = NULL;
			}

			virtual void InsertRecord(const size_t &thread_id, const size_t &table_id, char *data, const size_t &data_size) {
				char *buffer_ptr = buffers_[thread_id] + buffer_offsets_[thread_id];
				size_t &offset_ref = txn_offsets_[thread_id];
				memcpy(buffer_ptr + offset_ref, (char*)(&kInsert), sizeof(uint8_t));
				offset_ref += sizeof(uint8_t);
				memcpy(buffer_ptr + offset_ref, (char*)(&table_id), sizeof(size_t));
				offset_ref += sizeof(size_t);
				memcpy(buffer_ptr + offset_ref, (char*)(&data_size), sizeof(size_t));
				offset_ref += sizeof(size_t);
				memcpy(buffer_ptr + offset_ref, data, data_size);
				offset_ref += data_size;
			}

			virtual void UpdateRecord(const size_t &thread_id, const size_t &table_id, char *data, const size_t &data_size) {
				char *buffer_ptr = buffers_[thread_id] + buffer_offsets_[thread_id];
				size_t &offset_ref = txn_offsets_[thread_id];
				memcpy(buffer_ptr + offset_ref, (char*)(&kUpdate), sizeof(uint8_t));
				offset_ref += sizeof(uint8_t);
				memcpy(buffer_ptr + offset_ref, (char*)(&table_id), sizeof(size_t));
				offset_ref += sizeof(size_t);
				memcpy(buffer_ptr + offset_ref, (char*)(&data_size), sizeof(size_t));
				offset_ref += sizeof(size_t);
				memcpy(buffer_ptr + offset_ref, data, data_size);
				offset_ref += data_size;
			}

			virtual void DeleteRecord(const size_t &thread_id, const size_t &table_id, const std::string &primary_key) {
				char *buffer_ptr = buffers_[thread_id] + buffer_offsets_[thread_id];
				size_t &offset_ref = txn_offsets_[thread_id];
				memcpy(buffer_ptr + offset_ref, (char*)(&kDelete), sizeof(uint8_t));
				offset_ref += sizeof(uint8_t);
				memcpy(buffer_ptr + offset_ref, (char*)(&table_id), sizeof(size_t));
				offset_ref += sizeof(size_t);
				size_t size = primary_key.size();
				memcpy(buffer_ptr + offset_ref, (char*)(&size), sizeof(size_t));
				offset_ref += sizeof(size_t);
				memcpy(buffer_ptr + offset_ref, primary_key.c_str(), size);
				offset_ref += size;
			}

			virtual void CommitTransaction(const size_t &thread_id, const uint64_t &global_ts){
				char *buffer_ptr = buffers_[thread_id] + buffer_offsets_[thread_id];
				size_t tmp_size = txn_offsets_[thread_id] - sizeof(size_t);
				memcpy(buffer_ptr, (char*)(&tmp_size), sizeof(size_t));
				buffer_offsets_[thread_id] += txn_offsets_[thread_id];
				assert(buffer_offsets_[thread_id] < kValueLogBufferSize);
				if (global_ts != last_timestamps_[thread_id]){
					last_timestamps_[thread_id] = global_ts;
					fwrite(buffers_[thread_id], sizeof(char), buffer_offsets_[thread_id], outfiles_[thread_id]);
					int ret;
					ret = fflush(outfiles_[thread_id]);
					assert(ret == 0);
#if defined(__linux__)
					ret = fsync(fileno(outfiles_[thread_id]));
					assert(ret == 0);
#endif
					buffer_offsets_[thread_id] = 0;
				}
				txn_offsets_[thread_id] = sizeof(size_t);
			}

			virtual void AbortTransaction(const size_t &thread_id){
				txn_offsets_[thread_id] = sizeof(size_t);
			}

		private:
			ValueLogger(const ValueLogger &);
			ValueLogger& operator=(const ValueLogger &);

		private:
			char **buffers_;
			size_t *buffer_offsets_;
			size_t *txn_offsets_;
			uint64_t *last_timestamps_;
		};
	}
}

#endif
