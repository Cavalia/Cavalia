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
#include "../Meta/MetaConstants.h"

namespace Cavalia{
	namespace Database{
		struct ThreadBufferStruct{
#if !defined(COMPRESSION)
			ThreadBufferStruct(char *curr_buffer_ptr, const size_t &txn_header_size){
				buffer_ptr_ = curr_buffer_ptr;
				buffer_offset_ = 0;
				txn_offset_ = txn_header_size;
				last_epoch_ = 0;
			}
#else
			ThreadBufferStruct(char *curr_buffer_ptr, const size_t &txn_header_size, char *compressed_buffer_ptr){
				buffer_ptr_ = curr_buffer_ptr;
				buffer_offset_ = 0;
				txn_offset_ = txn_header_size;
				last_epoch_ = 0;

				compressed_buffer_ptr_ = compressed_buffer_ptr;
				//LZ4F_errorCode_t err = LZ4F_createCompressionContext(&compression_context_, LZ4F_VERSION);
				//assert(LZ4F_isError(err) == false);
			}
#endif
			char *buffer_ptr_;
			size_t buffer_offset_;
			// for value logging.
			size_t txn_offset_;
			uint64_t last_epoch_;
#if defined(COMPRESSION)
			char *compressed_buffer_ptr_;
			//LZ4F_compressionContext_t compression_context_;
#endif
		};

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
				thread_buf_structs_ = new ThreadBufferStruct*[thread_count_];
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
				delete[] thread_buf_structs_;
				thread_buf_structs_ = NULL;
			}

			void RegisterThread(const size_t &thread_id, const size_t &core_id){
				size_t numa_node_id = GetNumaNodeId(core_id);
				char *curr_buffer_ptr = MemAllocator::AllocNode(kLogBufferSize, numa_node_id);
#if defined(COMPRESSION)
				char *compressed_buffer_ptr = MemAllocator::AllocNode(kLogBufferSize, numa_node_id);
				thread_buf_structs_[thread_id] = (ThreadBufferStruct*)MemAllocator::AllocNode(sizeof(ThreadBufferStruct), numa_node_id);
				new(thread_buf_structs_[thread_id])ThreadBufferStruct(curr_buffer_ptr, txn_header_size_, compressed_buffer_ptr);
#else
				thread_buf_structs_[thread_id] = (ThreadBufferStruct*)MemAllocator::AllocNode(sizeof(ThreadBufferStruct), numa_node_id);
				new(thread_buf_structs_[thread_id])ThreadBufferStruct(curr_buffer_ptr, txn_header_size_);
#endif
			}

			void InsertRecord(const size_t &thread_id, const uint8_t &table_id, char *data, const uint8_t &data_size) {
				ThreadBufferStruct *buf_struct_ptr = thread_buf_structs_[thread_id];
				char *curr_buffer_ptr = buf_struct_ptr->buffer_ptr_ + buf_struct_ptr->buffer_offset_ + buf_struct_ptr->txn_offset_;
				memcpy(curr_buffer_ptr, (char*)(&kInsert), sizeof(uint8_t));
				memcpy(curr_buffer_ptr + sizeof(uint8_t), (char*)(&table_id), sizeof(uint8_t));
				memcpy(curr_buffer_ptr + sizeof(uint8_t) + sizeof(uint8_t), (char*)(&data_size), sizeof(uint8_t));
				memcpy(curr_buffer_ptr + sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint8_t), data, data_size);
				buf_struct_ptr->txn_offset_ += sizeof(uint8_t) * 3 + data_size;
			}

			void UpdateRecord(const size_t &thread_id, const uint8_t &table_id, char *data, const uint8_t &data_size) {
				ThreadBufferStruct *buf_struct_ptr = thread_buf_structs_[thread_id];
				char *curr_buffer_ptr = buf_struct_ptr->buffer_ptr_ + buf_struct_ptr->buffer_offset_ + buf_struct_ptr->txn_offset_;
				memcpy(curr_buffer_ptr, (char*)(&kUpdate), sizeof(uint8_t));
				memcpy(curr_buffer_ptr + sizeof(uint8_t), (char*)(&table_id), sizeof(uint8_t));
				memcpy(curr_buffer_ptr + sizeof(uint8_t) + sizeof(uint8_t), (char*)(&data_size), sizeof(uint8_t));
				memcpy(curr_buffer_ptr + sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint8_t), data, data_size);
				buf_struct_ptr->txn_offset_ += sizeof(uint8_t) * 3 + data_size;
			}

			void DeleteRecord(const size_t &thread_id, const uint8_t &table_id, const std::string &primary_key) {
				size_t key_size = primary_key.size();
				ThreadBufferStruct *buf_struct_ptr = thread_buf_structs_[thread_id];
				char *curr_buffer_ptr = buf_struct_ptr->buffer_ptr_ + buf_struct_ptr->buffer_offset_ + buf_struct_ptr->txn_offset_;
				memcpy(curr_buffer_ptr, (char*)(&kDelete), sizeof(uint8_t));
				memcpy(curr_buffer_ptr + sizeof(uint8_t), (char*)(&table_id), sizeof(uint8_t));
				memcpy(curr_buffer_ptr + sizeof(uint8_t) + sizeof(uint8_t), (char*)(&key_size), sizeof(uint8_t));
				memcpy(curr_buffer_ptr + sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint8_t), primary_key.c_str(), key_size);
				buf_struct_ptr->txn_offset_ += sizeof(uint8_t) * 3 + key_size;
			}

			// commit value logging.
			virtual void CommitTransaction(const size_t &thread_id, const uint64_t &epoch, const uint64_t &commit_ts) = 0;

			// abort value logging.
			void AbortTransaction(const size_t &thread_id){
				thread_buf_structs_[thread_id]->txn_offset_ = txn_header_size_;
			}

			// commit command logging.
			virtual void CommitTransaction(const size_t &thread_id, const uint64_t &epoch, const uint64_t &commit_ts, const size_t &txn_type, TxnParam *param) = 0;

			void CleanUp(const size_t &thread_id){
				FILE *file_ptr = outfiles_[thread_id];
				ThreadBufferStruct *buf_struct_ptr = thread_buf_structs_[thread_id];
#if defined(COMPRESSION)
				size_t &buffer_offset_ref = buf_struct_ptr->buffer_offset_;
				char *compressed_buffer_ptr = buf_struct_ptr->compressed_buffer_ptr_;
				size_t bound = LZ4F_compressFrameBound(buffer_offset_ref, NULL);
				size_t n = LZ4F_compressFrame(compressed_buffer_ptr, bound, buf_struct_ptr->buffer_ptr_, buffer_offset_ref, NULL);
				assert(LZ4F_isError(n) == false);

				// after compression, write into file
				fwrite(compressed_buffer_ptr, sizeof(char), n, file_ptr);
#else
				fwrite(buf_struct_ptr->buffer_ptr_, sizeof(char), buf_struct_ptr->buffer_offset_, file_ptr);
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
			ThreadBufferStruct **thread_buf_structs_;
			size_t txn_header_size_; 

		};
	}
}

#endif
