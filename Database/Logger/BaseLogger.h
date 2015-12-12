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
#include <NumaHelper.h>
#include "../Transaction/TxnParam.h"
#include "../Meta/MetaConstants.h"

namespace Cavalia{
	namespace Database{
		struct ThreadBufferStruct{
#if !defined(COMPRESSION)
			ThreadBufferStruct(const size_t &txn_header_size){
				buffer_offset_ = 0;
				txn_offset_ = txn_header_size;
				last_epoch_ = 0;
			}
#else
			ThreadBufferStruct(const size_t &txn_header_size, char *compressed_buffer, const size_t &compressed_buf_size){
				buffer_offset_ = 0;
				txn_offset_ = txn_header_size;
				last_epoch_ = 0;

				LZ4F_errorCode_t err = LZ4F_createCompressionContext(&compression_context_, LZ4F_VERSION);
				assert(LZ4F_isError(err) == false);
				// compress begin: put header
				compressed_buf_offset_ = LZ4F_compressBegin(compression_context_, compressed_buffer, compressed_buf_size, NULL);
			}
#endif
			size_t buffer_offset_;
			size_t txn_offset_;
			uint64_t last_epoch_;
#if defined(COMPRESSION)
			LZ4F_compressionContext_t compression_context_;
			size_t compressed_buf_offset_;
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
				buffers_ = new char*[thread_count_];
#if defined(COMPRESSION)
				size_t frame_size = LZ4F_compressBound(kLogBufferSize, NULL);
				// compressed_buf_size_ is the max size of file write
				compressed_buf_size_ = frame_size + LZ4_HEADER_SIZE + LZ4_FOOTER_SIZE;
				compressed_buffers_ = new char*[thread_count_];
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
				delete[] thread_buf_structs_;
				thread_buf_structs_ = NULL;
			}

			void RegisterThread(const size_t &thread_id, const size_t &core_id){
				size_t numa_node_id = GetNumaNodeId(core_id);
				buffers_[thread_id] = MemAllocator::AllocNode(kLogBufferSize, numa_node_id);
#if defined(COMPRESSION)
				compressed_buffers_[thread_id] = MemAllocator::AllocNode(compressed_buf_size_, numa_node_id);
				thread_buf_structs_[thread_id] = (ThreadBufferStruct*)MemAllocator::AllocNode(sizeof(ThreadBufferStruct), numa_node_id);
				new(thread_buf_structs_[thread_id])ThreadBufferStruct(txn_header_size_, compressed_buffers_[thread_id], compressed_buf_size_);
#else
				thread_buf_structs_[thread_id] = (ThreadBufferStruct*)MemAllocator::AllocNode(sizeof(ThreadBufferStruct), numa_node_id);
				new(thread_buf_structs_[thread_id])ThreadBufferStruct(txn_header_size_);
#endif
			}

			void InsertRecord(const size_t &thread_id, const uint8_t &table_id, char *data, const uint8_t &data_size) {
				ThreadBufferStruct *buf_struct_ptr = thread_buf_structs_[thread_id];
				char *buffer_ptr = buffers_[thread_id] + buf_struct_ptr->buffer_offset_;
				size_t &offset_ref = buf_struct_ptr->txn_offset_;
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
				ThreadBufferStruct *buf_struct_ptr = thread_buf_structs_[thread_id];
				char *buffer_ptr = buffers_[thread_id] + buf_struct_ptr->buffer_offset_;
				size_t &offset_ref = buf_struct_ptr->txn_offset_;
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
				ThreadBufferStruct *buf_struct_ptr = thread_buf_structs_[thread_id];
				char *buffer_ptr = buffers_[thread_id] + buf_struct_ptr->buffer_offset_;
				size_t &offset_ref = buf_struct_ptr->txn_offset_;
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
				thread_buf_structs_[thread_id]->txn_offset_ = txn_header_size_;
			}

			// commit command logging.
			void CommitTransaction(const size_t &thread_id, const uint64_t &epoch, const uint64_t &commit_ts, const size_t &txn_type, TxnParam *param){
				ThreadBufferStruct *buf_struct_ptr = thread_buf_structs_[thread_id];
				char *buffer_ptr = buffers_[thread_id];
				size_t &offset_ref = buf_struct_ptr->buffer_offset_;
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

				if (epoch != buf_struct_ptr->last_epoch_){
					FILE *file_ptr = outfiles_[thread_id];
					buf_struct_ptr->last_epoch_ = epoch;
#if defined(COMPRESSION)
					size_t& offset = buf_struct_ptr->compressed_buf_offset_;
					size_t n = LZ4F_compressUpdate(buf_struct_ptr->compression_context_, compressed_buffers_[thread_id] + offset, compressed_buf_size_ - offset, buffers_[thread_id], offset_ref, NULL);
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

			void CleanUp(const size_t &thread_id){
				FILE *file_ptr = outfiles_[thread_id];
				ThreadBufferStruct *buf_struct_ptr = thread_buf_structs_[thread_id];
#if defined(COMPRESSION)
				size_t& offset = buf_struct_ptr->compressed_buf_offset_;
				size_t n = LZ4F_compressUpdate(buf_struct_ptr->compression_context_, compressed_buffers_[thread_id] + offset, compressed_buf_size_ - offset, buffers_[thread_id], buf_struct_ptr->buffer_offset_, NULL);
				assert(LZ4F_isError(n) == false);
				offset += n;
				// after compression, write into file
				fwrite(compressed_buffers_[thread_id], sizeof(char), offset, file_ptr);
				offset = 0;

				n = LZ4F_compressEnd(buf_struct_ptr->compression_context_, compressed_buffers_[thread_id] + offset, compressed_buf_size_ - offset, NULL);
				assert(LZ4F_isError(n) == false);
				offset += n;
				fwrite(compressed_buffers_[thread_id], sizeof(char), offset, file_ptr);
				LZ4F_freeCompressionContext(buf_struct_ptr->compression_context_);
#else
				fwrite(buffers_[thread_id], sizeof(char), buf_struct_ptr->buffer_offset_, file_ptr);
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
			char **buffers_;
#if defined(COMPRESSION)
			char **compressed_buffers_;
			size_t compressed_buf_size_;
#endif
			size_t txn_header_size_;
		};
	}
}

#endif
