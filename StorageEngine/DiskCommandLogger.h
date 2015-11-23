//#pragma once
//#ifndef __CAVALIA_STORAGE_ENGINE_COMMAND_LOGGER_H__
//#define __CAVALIA_STORAGE_ENGINE_COMMAND_LOGGER_H__
//
//#include <fstream>
//#include "BaseLogger.h"
//
//namespace Cavalia {
//	namespace StorageEngine {
//		class CommandLogger : public BaseLogger {
//		public:
//			CommandLogger(const std::string &filename, size_t commit_batch = 1) : commit_batch_(commit_batch) {
//				output_file_.open(filename, std::ofstream::binary);
//			}
//			virtual ~CommandLogger() {
//				output_file_.close();
//			}
//
//			virtual void InsertRecord(const size_t &thread_id, const size_t &table_id, char *data, const size_t &data_size) {}
//			virtual void UpdateRecord(const size_t &thread_id, const size_t &table_id, char *data, const size_t &data_size) {}
//			virtual void DeleteRecord(const size_t &thread_id, const size_t &table_id, const std::string &primary_key) {}
//			virtual void CommitTransaction(const size_t &thread_id, const uint64_t &commit_ts) {}
//
//			virtual void CommitTransaction(const size_t &thread_id, const uint64_t &commit_ts, const size_t &txn_type, EventTuple *param) {
//				CharArray param_chars;
//				param->Serialize(param_chars);
//				// write stored procedure type.
//				output_file_.write((char*)(&txn_type), sizeof(txn_type));
//				// write parameter size.
//				output_file_.write((char*)(&param_chars.size_), sizeof(param_chars.size_));
//				// write parameters.
//				output_file_.write(param_chars.char_ptr_, param_chars.size_);
//				output_file_.flush();
//				param_chars.Release();
//			}
//
//		private:
//			CommandLogger(const CommandLogger &);
//			CommandLogger& operator=(const CommandLogger &);
//
//		private:
//			const size_t commit_batch_;
//			std::ofstream output_file_;
//		};
//	}
//}
//
//#endif
