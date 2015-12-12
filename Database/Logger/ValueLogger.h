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
				char *buffer_ptr = buffers_[thread_id] + *(buffer_offsets_[thread_id]);
				memcpy(buffer_ptr, (char*)(txn_offsets_[thread_id]), sizeof(size_t));
				memcpy(buffer_ptr + sizeof(size_t), (char*)(&commit_ts), sizeof(uint64_t));
				*(buffer_offsets_[thread_id]) += *(txn_offsets_[thread_id]);
				assert(*(buffer_offsets_[thread_id]) < kLogBufferSize);
				if (epoch != *(last_epochs_[thread_id])){
					FILE *file_ptr = outfiles_[thread_id];
					*(last_epochs_[thread_id]) = epoch;
#if defined(COMPRESSION)
					size_t& offset = *(compressed_buf_offsets_[thread_id]);
					size_t n = LZ4F_compressUpdate(*(compression_contexts_[thread_id]), compressed_buffers_[thread_id] + offset, compressed_buf_size_ - offset, buffers_[thread_id], *(buffer_offsets_[thread_id]), NULL);
					assert(LZ4F_isError(n) == false);
					offset += n;

					// after compression, write into file
					fwrite(compressed_buffers_[thread_id], sizeof(char), offset, file_ptr);
					offset = 0;
#else
					//fwrite(buffers_[thread_id], sizeof(char), *(buffer_offsets_[thread_id]), file_ptr);
#endif
					int ret;
					ret = fflush(file_ptr);
					assert(ret == 0);
#if defined(__linux__)
					ret = fsync(fileno(file_ptr));
					assert(ret == 0);
#endif
					*(buffer_offsets_[thread_id]) = 0;
				}
				*(txn_offsets_[thread_id]) = sizeof(size_t)+sizeof(uint64_t);
			}

			virtual void CommitTransaction(const size_t &thread_id, const uint64_t &epoch, const uint64_t &commit_ts, const size_t &txn_type, TxnParam *param){}

		private:
			ValueLogger(const ValueLogger &);
			ValueLogger& operator=(const ValueLogger &);
		};
	}
}

#endif
