#pragma once
#ifndef __CAVALIA_DATABASE_BASE_LOGGER_H__
#define __CAVALIA_DATABASE_BASE_LOGGER_H__

#include <atomic>
#include <string>
#include <cstdio>
#if defined(__linux__)
#include <unistd.h>
#endif
#if defined(COMPRESSION)
#include <lz4frame.h>
#endif
#include <AllocatorHelper.h>
#include <NumaHelper.h>
#include "../Transaction/TxnParam.h"
#include "../Transaction/TxnAccess.h"
#include "../Meta/MetaTypes.h"
#include "ThreadLogBuffer.h"

namespace Cavalia{
	namespace Database{
		class BaseLogger{
		public:
			BaseLogger(const std::string &dir_name, const size_t &thread_count, bool is_vl) : dir_name_(dir_name), thread_count_(thread_count){
				outfiles_ = new FILE*[thread_count_];
				// is value logging
				if (is_vl == true){
					for (size_t i = 0; i < thread_count_; ++i){
#if defined(COMPRESSION)
						outfiles_[i] = fopen((dir_name_ + "/valuelog-compress-" + std::to_string(i)).c_str(), "wb");
#else
						outfiles_[i] = fopen((dir_name_ + "/valuelog-" + std::to_string(i)).c_str(), "wb");
#endif
						assert(outfiles_[i] != NULL);
					}
				}
				// is command logging
				else{
					for (size_t i = 0; i < thread_count_; ++i){
#if defined(COMPRESSION)
						outfiles_[i] = fopen((dir_name_ + "/commandlog-compress-" + std::to_string(i)).c_str(), "wb");
#else
						outfiles_[i] = fopen((dir_name_ + "/commandlog-" + std::to_string(i)).c_str(), "wb");
#endif
						assert(outfiles_[i] != NULL);
					}
				}
				thread_log_buffer_ = new ThreadLogBuffer*[thread_count_];
			}
			virtual ~BaseLogger(){
				for (size_t i = 0; i < thread_count_; ++i){
					int ret;
					ret = fflush(outfiles_[i]);
					assert(ret == 0);
#if defined(__linux__)
					ret = fsync(fileno(outfiles_[i]));
					assert(ret == 0);
#endif
					ret = fclose(outfiles_[i]);
					assert(ret == 0);
				}
				delete[] outfiles_;
				outfiles_ = NULL;
				delete[] thread_log_buffer_;
				thread_log_buffer_ = NULL;
			}

			void RegisterThread(const size_t &thread_id, const size_t &core_id){
				size_t numa_node_id = GetNumaNodeId(core_id);
				char *buffer_ptr = MemAllocator::AllocNode(kLogBufferSize, numa_node_id);
#if defined(COMPRESSION)
				char *compressed_buffer_ptr = MemAllocator::AllocNode(kLogBufferSize, numa_node_id);
				thread_log_buffer_[thread_id] = (ThreadLogBuffer*)MemAllocator::AllocNode(sizeof(ThreadLogBuffer), numa_node_id);
				new(thread_log_buffer_[thread_id])ThreadLogBuffer(buffer_ptr, compressed_buffer_ptr);
#else
				thread_log_buffer_[thread_id] = (ThreadLogBuffer*)MemAllocator::AllocNode(sizeof(ThreadLogBuffer), numa_node_id);
				new(thread_log_buffer_[thread_id])ThreadLogBuffer(buffer_ptr);
#endif
			}

			// commit value logging.
			virtual void CommitTransaction(const size_t &thread_id, const uint64_t &epoch, const uint64_t &commit_ts, AccessList<kMaxAccessNum> &access_list) = 0;

			// commit command logging.
			virtual void CommitTransaction(const size_t &thread_id, const uint64_t &epoch, const uint64_t &commit_ts, const size_t &txn_type, TxnParam *param) = 0;

			void CleanUp(const size_t &thread_id){
				ThreadLogBuffer *buf_struct_ptr = thread_log_buffer_[thread_id];
				size_t &buffer_offset_ref = buf_struct_ptr->buffer_offset_;
				FILE *file_ptr = outfiles_[thread_id];
				int result;
				// record epoch.
				uint64_t punctuation = -1;
				result = fwrite(&punctuation, sizeof(uint64_t), 1, file_ptr);
				assert(result == 1);
#if defined(COMPRESSION)
				char *compressed_buffer_ptr = buf_struct_ptr->compressed_buffer_ptr_;
				size_t bound = LZ4F_compressFrameBound(buffer_offset_ref, NULL);
				size_t n = LZ4F_compressFrame(compressed_buffer_ptr, bound, buf_struct_ptr->buffer_ptr_, buffer_offset_ref, NULL);
				assert(LZ4F_isError(n) == false);

				// after compression, write into file
				result = fwrite(&n, sizeof(size_t), 1, file_ptr);
				assert(result == 1);
				result = fwrite(compressed_buffer_ptr, sizeof(char), n, file_ptr);
				assert(result == n);
#else
				result = fwrite(&buffer_offset_ref, sizeof(size_t), 1, file_ptr);
				assert(result == 1);
				result = fwrite(buf_struct_ptr->buffer_ptr_, sizeof(char), buffer_offset_ref, file_ptr);
				assert(result == buffer_offset_ref);
#endif
				result = fflush(file_ptr);
				assert(result == 0);
#if defined(__linux__)
				result = fsync(fileno(file_ptr));
				assert(result == 0);
#endif
			}

		private:
			BaseLogger(const BaseLogger &);
			BaseLogger& operator=(const BaseLogger &);

		protected:
			std::string dir_name_;
			size_t thread_count_;
			FILE **outfiles_;

		protected:
			ThreadLogBuffer **thread_log_buffer_;
		};
	}
}

#endif
