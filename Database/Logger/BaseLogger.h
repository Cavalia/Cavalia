#pragma once
#ifndef __CAVALIA_DATABASE_BASE_LOGGER_H__
#define __CAVALIA_DATABASE_BASE_LOGGER_H__

#include <string>
#include <cstdio>
#if defined(__linux__)
#include <unistd.h>
#endif
#if defined(COMPRESSION)
#include <lz4frame.h>
#endif
#include <NumaHelper.h>
#include "../Transaction/TxnParam.h"
#include "../Meta/MetaConstants.h"

namespace Cavalia{
	namespace Database{
		class BaseLogger{
		public:
			BaseLogger(const std::string &dir_name, const size_t &thread_count, bool is_vl) : dir_name_(dir_name), thread_count_(thread_count){
				outfiles_ = new FILE*[thread_count_];
				// is value logging
				if (is_vl == true){
					for (size_t i = 0; i < thread_count_; ++i){
						outfiles_[i] = fopen((dir_name_ + "/valuelog" + std::to_string(i)).c_str(), "wb");
						assert(outfiles_[i] != NULL);
					}
				}
				// is command logging
				else{
					for (size_t i = 0; i < thread_count_; ++i){
						outfiles_[i] = fopen((dir_name_ + "/commandlog" + std::to_string(i)).c_str(), "wb");
						assert(outfiles_[i] != NULL);
					}
				}

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
				compressed_buf_offsets_ = new size_t*[thread_count_];
#endif
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
				compressed_buf_offsets_[thread_id] = (size_t*)(MemAllocator::AllocNode(sizeof(size_t), numa_node_id));
				*(compressed_buf_offsets_[thread_id]) = LZ4F_compressBegin(*(compression_contexts_[thread_id]), compressed_buffers_[thread_id], compressed_buf_size_, NULL);
#endif
			}

			void InsertRecord(const size_t &thread_id, const uint8_t &table_id, char *data, const uint8_t &data_size) {
				char *buffer_ptr = buffers_[thread_id] + *(buffer_offsets_[thread_id]);
				size_t &offset_ref = *(txn_offsets_[thread_id]);
				memcpy(buffer_ptr + offset_ref, (char*)(&kInsert), sizeof(uint8_t));
				offset_ref += sizeof(uint8_t);
				memcpy(buffer_ptr + offset_ref, (char*)(&table_id), sizeof(uint8_t));
				offset_ref += sizeof(uint8_t);
				memcpy(buffer_ptr + offset_ref, (char*)(&data_size), sizeof(uint8_t));
				offset_ref += sizeof(uint8_t);
				memcpy(buffer_ptr + offset_ref, data, data_size);
				offset_ref += data_size;
			}

			void UpdateRecord(const size_t &thread_id, const uint8_t &table_id, char *data, const uint8_t &data_size) {
				char *buffer_ptr = buffers_[thread_id] + *(buffer_offsets_[thread_id]);
				size_t &offset_ref = *(txn_offsets_[thread_id]);
				memcpy(buffer_ptr + offset_ref, (char*)(&kUpdate), sizeof(uint8_t));
				offset_ref += sizeof(uint8_t);
				memcpy(buffer_ptr + offset_ref, (char*)(&table_id), sizeof(uint8_t));
				offset_ref += sizeof(uint8_t);
				memcpy(buffer_ptr + offset_ref, (char*)(&data_size), sizeof(uint8_t));
				offset_ref += sizeof(uint8_t);
				memcpy(buffer_ptr + offset_ref, data, data_size);
				offset_ref += data_size;
			}

			void DeleteRecord(const size_t &thread_id, const uint8_t &table_id, const std::string &primary_key) {
				char *buffer_ptr = buffers_[thread_id] + *(buffer_offsets_[thread_id]);
				size_t &offset_ref = *(txn_offsets_[thread_id]);
				memcpy(buffer_ptr + offset_ref, (char*)(&kDelete), sizeof(uint8_t));
				offset_ref += sizeof(uint8_t);
				memcpy(buffer_ptr + offset_ref, (char*)(&table_id), sizeof(uint8_t));
				offset_ref += sizeof(uint8_t);
				size_t size = primary_key.size();
				memcpy(buffer_ptr + offset_ref, (char*)(&size), sizeof(uint8_t));
				offset_ref += sizeof(uint8_t);
				memcpy(buffer_ptr + offset_ref, primary_key.c_str(), size);
				offset_ref += size;
			}

			// commit value logging.
			virtual void CommitTransaction(const size_t &thread_id, const uint64_t &epoch, const uint64_t &commit_ts) = 0;

			// abort value logging.
			void AbortTransaction(const size_t &thread_id){
				*(txn_offsets_[thread_id]) = sizeof(size_t)+sizeof(uint64_t);
			}

			// commit command logging.
			virtual void CommitTransaction(const size_t &thread_id, const uint64_t &epoch, const uint64_t &commit_ts, const size_t &txn_type, TxnParam *param) = 0;

			void CleanUp(const size_t &thread_id){
				FILE *file_ptr = outfiles_[thread_id];
#if defined(COMPRESSION)
				size_t& offset = *(compressed_buf_offsets_[thread_id]);
				size_t n = LZ4F_compressUpdate(*(compression_contexts_[thread_id]), compressed_buffers_[thread_id] + offset, compressed_buf_size_ - offset, buffers_[thread_id], *(buffer_offsets_[thread_id]), NULL);
				assert(LZ4F_isError(n) == false);
				offset += n;
				// after compression, write into file
				fwrite(compressed_buffers_[thread_id], sizeof(char), offset, file_ptr);
				offset = 0;

				n = LZ4F_compressEnd(*(compression_contexts_[thread_id]), compressed_buffers_[thread_id] + offset, compressed_buf_size_ - offset, NULL);
				assert(LZ4F_isError(n) == false);
				offset += n;
				fwrite(compressed_buffers_[thread_id], sizeof(char), offset, file_ptr);
				LZ4F_freeCompressionContext(*(compression_contexts_[thread_id]));
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
			}

		private:
			BaseLogger(const BaseLogger &);
			BaseLogger& operator=(const BaseLogger &);

		protected:
			std::string dir_name_;
			size_t thread_count_;
			FILE **outfiles_;

		protected:
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
