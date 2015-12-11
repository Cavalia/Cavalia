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
				buffer_offsets_ = new size_t*[thread_count_];
				txn_offsets_ = new size_t*[thread_count_];
				last_epochs_ = new uint64_t*[thread_count_];
#if defined(COMPRESSION)
				size_t frame_size = LZ4F_compressBound(kLogBufferSize, NULL);
				// compressed_buf_size_ is the max size of file write
				compressed_buf_size_ = frame_size + LZ4_HEADER_SIZE + LZ4_FOOTER_SIZE;

				compression_contexts_ = new LZ4F_compressionContext_t*[thread_count_];
				compressed_buffers_ = new char*[thread_count_];
				compressed_buf_offsets_ = new size_t[thread_count_];
#endif
			}

			virtual ~CommandLogger() {
//#if defined(COMPRESSION)
//				for (size_t i = 0; i < thread_count_; ++i){
//					size_t& offset = compressed_buf_offsets_[i];
//					size_t n = LZ4F_compressEnd(compression_contexts_[i], compressed_buffers_[i] + offset, compressed_buf_size_ - offset, NULL);
//					assert(LZ4F_isError(n) == false);
//					offset += n;
//
//					FILE *file_ptr = outfiles_[i];
//					fwrite(compressed_buffers_[i], sizeof(char), offset, file_ptr);
//
//					LZ4F_freeCompressionContext(compression_contexts_[i]);
//				}
//				delete[] compression_contexts_;
//				compression_contexts_ = NULL;
//				for (size_t i = 0; i < thread_count_; ++i){
//					delete[] compressed_buffers_[i];
//					compressed_buffers_[i] = NULL;
//				}
//				delete[] compressed_buffers_;
//				compressed_buffers_ = NULL;
//				delete[] compressed_buf_offsets_;
//				compressed_buf_offsets_ = NULL;
//#endif
				//for (size_t i = 0; i < thread_count_; ++i){
				//	delete[] buffers_[i];
				//	buffers_[i] = NULL;
				//}
				//delete[] buffers_;
				//buffers_ = NULL;
				//delete[] buffer_offsets_;
				//buffer_offsets_ = NULL;
				//delete[] txn_offsets_;
				//txn_offsets_ = NULL;
				//delete[] last_epochs_;
				//last_epochs_ = NULL;
			}

			void RegisterThread(const size_t &thread_id, const size_t &core_id){
				size_t numa_node_id = GetNumaNodeId(core_id);
				buffers_[thread_id] = MemAllocator::AllocNode(kLogBufferSize, numa_node_id);
				buffer_offsets_[thread_id] = (size_t*)MemAllocator::AllocNode(sizeof(size_t), numa_node_id);
				txn_offsets_[thread_id] = (size_t*)MemAllocator::AllocNode(sizeof(size_t), numa_node_id);
				last_epochs_[thread_id] = (uint64_t*)MemAllocator::AllocNode(sizeof(uint64_t), numa_node_id);
				*(buffer_offsets_[thread_id]) = 0;
				*(txn_offsets_[thread_id]) = sizeof(size_t)+sizeof(uint64_t);
				*(last_epochs_[thread_id]) = 0;

#if defined(COMPRESSION)
				compression_contexts_[thread_id] = (LZ4F_compressionContext_t*)(MemAllocator::AllocNode(sizeof(LZ4F_compressionContext_t), numa_node_id));
				new(compression_contexts_[thread_id])LZ4F_compressionContext_t();
				LZ4F_errorCode_t err = LZ4F_createCompressionContext(compression_contexts_[thread_id], LZ4F_VERSION);
				assert(LZ4F_isError(err) == false);
				compressed_buffers_[thread_id] = new char[compressed_buf_size_];
				// compress begin: put header
				compressed_buf_offsets_[thread_id] = LZ4F_compressBegin(compression_contexts_[thread_id], compressed_buffers_[thread_id], compressed_buf_size_, NULL);
#endif
			}

			void CommitTransaction(const size_t &thread_id, const uint64_t &epoch, const uint64_t &commit_ts, const size_t &txn_type, TxnParam *param) {
				if (epoch == -1){
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
					fwrite(buffers_[thread_id], sizeof(char), *(buffer_offsets_[thread_id]), file_ptr);
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
				size_t &offset_ref = *(buffer_offsets_[thread_id]);
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

				if (epoch != *(last_epochs_[thread_id]) || epoch == (uint64_t)(-1)){
					FILE *file_ptr = outfiles_[thread_id];
					*(last_epochs_[thread_id]) = epoch;
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
			size_t **buffer_offsets_;
			size_t **txn_offsets_;
			uint64_t **last_epochs_;
#if defined(COMPRESSION)
			LZ4F_compressionContext_t **compression_contexts_;
			char **compressed_buffers_;
			size_t **compressed_buf_offsets_;
			size_t compressed_buf_size_;
#endif
		};
	}
}

#endif
