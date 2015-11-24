//#pragma once
//#ifndef __CAVALIA_DATABASE_DISK_VALUE_LOGGER_H__
//#define __CAVALIA_DATABASE_DISK_VALUE_LOGGER_H__
//
//#include <fstream>
//#include <boost/thread/mutex.hpp>
//#include <boost/atomic.hpp>
//
//#include "BaseLogger.h"
//#include "LogEntry.h"
//#include "Serialization/LogEntryDump.pb.h"
//
//namespace Cavalia{
//	namespace Database{
//		const size_t kBufferSize = 4096000;
//		class DiskValueLogger : public BaseLogger{
//		public:
//			DiskValueLogger(const std::string &filename, size_t commit_batch = 1) : commit_batch_(commit_batch){
//				commit_count_ = 0;
//				output_file_.open(filename, std::ofstream::binary);
//				buffer_.Allocate(kBufferSize);
//				buffer_offset_ = 0;
//				memset(&spinlock_, 0, sizeof(spinlock_));
//			}
//			virtual ~DiskValueLogger(){
//				output_file_.write(reinterpret_cast<char*>(&buffer_offset_), sizeof(buffer_offset_));
//				output_file_.write(buffer_.char_ptr_, buffer_offset_);
//				output_file_.flush();
//				buffer_.Clear();
//				
//				output_file_.close();
//			}
//
//			virtual void InsertRecord(const int64_t &txn_id, const size_t &table_id, const SchemaRecord &record){
//				AcquireLock();
//				log_entry_dump_.set_opt_type(LogEntryDump_OptType_INSERT);
//				log_entry_dump_.set_table_id(table_id);
//				log_entry_dump_.set_primary_key(record.GetPrimaryKey());
//				size_t record_size = record.GetRecordSize();
//				log_entry_dump_.set_val_len(record_size);
//				log_entry_dump_.set_new_val(std::string(record.data_ptr_, record_size));
//				FlushToBuffer();
//				ReleaseLock();
//			}
//			virtual void DeleteRecord(const int64_t &txn_id, const size_t &table_id, const SchemaRecord &record){
//				assert(false);
//			}
//			virtual void UpdateRecord(const int64_t &txn_id, const size_t &table_id, const SchemaRecord &record, const ColumnPredicate &param){
//				AcquireLock();
//				log_entry_dump_.set_opt_type(LogEntryDump_OptType_UPDATE);
//				log_entry_dump_.set_table_id(table_id);
//				log_entry_dump_.set_primary_key(record.GetPrimaryKey());
//				log_entry_dump_.set_column_id(0);
//				size_t record_size = record.GetRecordSize();
//				log_entry_dump_.set_val_len(record_size);
//				//log_entry_dump_.set_old_val(std::string(record.FetchData(), record_size));
//				log_entry_dump_.set_new_val(std::string(record.data_ptr_, record_size));
//				FlushToBuffer();
//				ReleaseLock();
//			}
//			virtual void UpdateRecord(const int64_t &txn_id, const size_t &table_id, const SchemaRecord &record, const ColumnPredicates &params){
//				assert(false);
//			}
//
//			virtual void CommitTransaction(const size_t &txn_type, EventTuple *param){
//				AcquireLock();
//				commit_count_ += 1;
//				if (commit_count_%commit_batch_ == 0){
//					output_file_.write(reinterpret_cast<char*>(&buffer_offset_), sizeof(buffer_offset_));
//					output_file_.write(buffer_.char_ptr_, buffer_offset_);
//					output_file_.flush();
//					buffer_.Clear();
//					buffer_offset_ = 0;
//					commit_count_ = 0;
//				}
//				ReleaseLock();
//			}
//
//			virtual void AbortTransaction(const int64_t &txn_id){
//				AcquireLock();
//				buffer_.Clear();
//				buffer_offset_ = 0;
//				ReleaseLock();
//			}
//		private:
//			void FlushToBuffer(){
//				size_t byte_size = log_entry_dump_.ByteSize();
//				log_entry_dump_.SerializeToArray(buffer_.char_ptr_ + buffer_offset_, byte_size);
//				log_entry_dump_.Clear();
//				buffer_offset_ += byte_size;
//			}
//			void AcquireLock(){
//				spinlock_.lock();
//			}
//			void ReleaseLock(){
//				spinlock_.unlock();
//			}
//
//		private:
//			DiskValueLogger(const DiskValueLogger &);
//			DiskValueLogger& operator=(const DiskValueLogger &);
//
//		private:
//			const size_t commit_batch_;
//			size_t commit_count_;
//			std::ofstream output_file_;
//
//			LogEntryDump log_entry_dump_;
//			CharArray buffer_;
//			size_t buffer_offset_;
//
//			boost::detail::spinlock spinlock_;
//		};
//	}
//}
//
//#endif
