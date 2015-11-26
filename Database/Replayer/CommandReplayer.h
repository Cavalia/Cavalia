#pragma once
#ifndef __CAVALIA_DATABASE_COMMAND_REPLAYER_H__
#define __CAVALIA_DATABASE_COMMAND_REPLAYER_H__

#include "../Storage/BaseStorageManager.h"
#include "BaseReplayer.h"

namespace Cavalia{
	namespace Database{
		class CommandReplayer : public BaseReplayer{
		public:
			CommandReplayer(const std::string &filename, BaseStorageManager *const storage_manager, const size_t &thread_count) : BaseReplayer(filename, storage_manager, thread_count, false){}
			virtual ~CommandReplayer(){}

			virtual void Start(){
				PrepareProcedures();
				TransactionManager *txn_manager = new TransactionManager(storage_manager_, NULL);
				StoredProcedure **procedures = new StoredProcedure*[registers_.size()];
				for (auto &entry : registers_){
					procedures[entry.first] = entry.second(0);
					procedures[entry.first]->SetTransactionManager(txn_manager);
				}
				ReloadLog();
				ProcessLog();
			}

		private:
			virtual void PrepareProcedures() = 0;
			virtual TxnParam* DeserializeParam(const size_t &param_type, const CharArray &entry) = 0;

			virtual void ReloadLog(){
				//std::ifstream log_reloader(filename_, std::ifstream::binary);
				//assert(log_reloader.good() == true);
				//log_reloader.seekg(0, std::ios::end);
				//size_t file_size = static_cast<size_t>(log_reloader.tellg());
				//log_reloader.seekg(0, std::ios::beg);
				//size_t file_pos = 0;
				//CharArray entry;
				//entry.Allocate(1024);
				//while (file_pos < file_size){
				//	size_t param_type;
				//	log_reloader.read(reinterpret_cast<char*>(&param_type), sizeof(param_type));
				//	file_pos += sizeof(param_type);
				//	log_reloader.read(reinterpret_cast<char*>(&entry.size_), sizeof(entry.size_));
				//	file_pos += sizeof(entry.size_);
				//	if (file_size - file_pos >= entry.size_){
				//		log_reloader.read(entry.char_ptr_, entry.size_);
				//		TxnParam* event_tuple = DeserializeParam(param_type, entry);
				//		if (event_tuple != NULL){
				//			log_buffer_.push_back(std::make_pair(param_type, event_tuple));
				//		}
				//		file_pos += entry.size_;
				//	}
				//	else{
				//		break;
				//	}
				//}
				//entry.Release();
				//log_reloader.close();
			}

			virtual void ProcessLog(){
				//std::string ret;
				//for (auto &log_pair : log_buffer_){
				//	procedures_[log_pair.first]->Execute(log_pair.second, ret);
				//}
			}

		private:
			CommandReplayer(const CommandReplayer &);
			CommandReplayer& operator=(const CommandReplayer &);

		protected:
			std::unordered_map<size_t, std::function<StoredProcedure*(size_t)>> registers_;
			std::unordered_map<size_t, std::function<void(char*)>> deregisters_;
		};
	}
}

#endif
