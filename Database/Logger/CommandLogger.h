#pragma once
#ifndef __CAVALIA_DATABASE_COMMAND_LOGGER_H__
#define __CAVALIA_DATABASE_COMMAND_LOGGER_H__

#include "../Transaction/TxnParam.h"
#include "BaseLogger.h"
#if defined(COMPRESSION)
#include <lz4frame.h>
#endif

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
#if defined(COMPRESSION)
				compression_contexts_ = new LZ4F_compressionContext_t[thread_count_];
				for (size_t i = 0; i < thread_count_; ++i){
					LZ4F_errorCode_t err = LZ4F_createCompressionContext(&compression_contexts_[i], LZ4F_VERSION);
					assert(LZ4F_isError(err) == false);
				}
				size_t frame_size = LZ4F_compressBound(kValueLogBufferSize, NULL);
				// compressed_buf_size_ is the max size of file write
				compressed_buf_size_ = frame_size + LZ4_HEADER_SIZE + LZ4_FOOTER_SIZE;
				
				compressed_buffers_ = new char*[thread_count_];
				compressed_buf_offsets_ = new size_t[thread_count_];
				for (size_t i = 0; i < thread_count_; ++i){
					compressed_buf_offsets_[i] = 0;
					compressed_buffers_[i] = new char[compressed_buf_size_];
				}
				
				// compress begin: put header
				for (size_t i = 0; i < thread_count_; ++i){
					compressed_buf_offsets_[i] = LZ4F_compressBegin(compression_contexts_[i], compressed_buffers_[i], compressed_buf_size_, NULL);
				}
#endif

			}
			virtual ~CommandLogger() {
#if defined(COMPRESSION)
				for (size_t i = 0; i < thread_count_; ++i){
					size_t& offset = compressed_buf_offsets_[i];
					size_t n = LZ4F_compressEnd(compression_contexts_[i], compressed_buffers_[i] + offset, compressed_buf_size_ - offset, NULL);
					assert(LZ4F_isError(n) == false);
					offset += n;
					
					FILE *file_ptr = outfiles_[i];
					fwrite(compressed_buffers_[i], sizeof(char), offset, file_ptr);

					LZ4F_freeCompressionContext(compression_contexts_[i]);
				}
				delete[] compression_contexts_;
				compression_contexts_ = NULL;
				for (size_t i = 0; i < thread_count_; ++i){
					delete[] compressed_buffers_[i];
					compressed_buffers_[i] = NULL;
				}
				delete[] compressed_buffers_;
				compressed_buffers_ = NULL;
				delete[] compressed_buf_offsets_;
				compressed_buf_offsets_ = NULL;
#endif
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

			void CommitTransaction(const size_t &thread_id, const uint64_t &global_ts, const uint64_t &commit_ts, const size_t &txn_type, TxnParam *param) {
				if (global_ts == -1){
					FILE *file_ptr = outfiles_[thread_id];
#if defined(COMPRESSION)
					size_t& offset = compressed_buf_offsets_[thread_id];
					size_t n = LZ4F_compressUpdate(compression_contexts_[thread_id], compressed_buffers_[thread_id] + offset, compressed_buf_size_ - offset, buffers_[thread_id], buffer_offsets_[thread_id], NULL);
					assert(LZ4F_isError(n) == false);
					offset += n;
					
					// after compression, write into file
					fwrite(compressed_buffers_[thread_id], sizeof(char), offset, file_ptr);
					offset = 0;
#else
					fwrite(buffers_[thread_id], sizeof(char), buffer_offsets_[thread_id], file_ptr);
#endif
					int ret;
					ret = fflush(file_ptr);
					assert(ret == 0);
#if defined(__linux__)
					ret = fsync(fileno(file_ptr));
					assert(ret == 0);
#endif
					return;
				}
				char *buffer_ptr = buffers_[thread_id];
				size_t &offset_ref = buffer_offsets_[thread_id];
				// write timestamp.
				memcpy(buffer_ptr + offset_ref, (char*)(&global_ts), sizeof(global_ts));
				offset_ref += sizeof(global_ts);
				// write stored procedure type.
				memcpy(buffer_ptr + offset_ref, (char*)(&txn_type), sizeof(txn_type));
				offset_ref += sizeof(txn_type);
				size_t tmp_size = 0;
				// write parameters. get tmp_size first.
				param->Serialize(buffer_ptr + offset_ref + sizeof(tmp_size), tmp_size);
				// write parameter size.
				memcpy(buffer_ptr + offset_ref, (char*)(&tmp_size), sizeof(tmp_size));
				offset_ref += sizeof(tmp_size)+tmp_size;

				if (global_ts != last_timestamps_[thread_id] || global_ts == (uint64_t)(-1)){
					FILE *file_ptr = outfiles_[thread_id];
					last_timestamps_[thread_id] = global_ts;
#if defined(COMPRESSION)
					size_t& offset = compressed_buf_offsets_[thread_id];
					size_t n = LZ4F_compressUpdate(compression_contexts_[thread_id], compressed_buffers_[thread_id] + offset, compressed_buf_size_ - offset, buffers_[thread_id], buffer_offsets_[thread_id], NULL);
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

		private:
			char **buffers_;
			size_t *buffer_offsets_;
			uint64_t *last_timestamps_;
#if defined(COMPRESSION)
			LZ4F_compressionContext_t* compression_contexts_;
			char **compressed_buffers_;
			size_t *compressed_buf_offsets_;
			size_t compressed_buf_size_;
#endif
		};
	}
}

#endif
