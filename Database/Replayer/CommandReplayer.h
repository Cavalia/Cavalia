#pragma once
#ifndef __CAVALIA_DATABASE_COMMAND_REPLAYER_H__
#define __CAVALIA_DATABASE_COMMAND_REPLAYER_H__

#include "../Storage/BaseStorageManager.h"
#include "BaseReplayer.h"

namespace Cavalia{
	namespace Database{
		class CommandReplayer : public BaseReplayer{
		public:
			CommandReplayer(const std::string &filename, BaseStorageManager *const storage_manager, const size_t &thread_count) : BaseReplayer(filename, storage_manager, thread_count, false){
				input_batches_ = new ParamBatch[thread_count_];
			}
			virtual ~CommandReplayer(){
				delete[] input_batches_;
				input_batches_ = NULL;
			}

			virtual void Start(){
				PrepareProcedures();
				TransactionManager *txn_manager = new TransactionManager(storage_manager_, NULL);
				StoredProcedure **procedures = new StoredProcedure*[registers_.size()];
				for (auto &entry : registers_){
					procedures[entry.first] = entry.second(0);
					procedures[entry.first]->SetTransactionManager(txn_manager);
				}
				boost::thread_group reloaders;
				for (size_t i = 0; i < thread_count_; ++i){
					reloaders.create_thread(boost::bind(&CommandReplayer::ReloadLog, this, i));
				}
				reloaders.join_all();
				ProcessLog();
			}

		private:
			virtual void PrepareProcedures() = 0;
			virtual TxnParam* DeserializeParam(const size_t &param_type, const CharArray &entry) = 0;

			void ReloadLog(const size_t &thread_id){
				std::ifstream &infile_ref = infiles_[thread_id];
				ParamBatch &input_batch = input_batches_[thread_id];
				infile_ref.seekg(0, std::ios::end);
				size_t file_size = static_cast<size_t>(infile_ref.tellg());
				infile_ref.seekg(0, std::ios::beg);
				size_t file_pos = 0;
				CharArray entry;
				entry.Allocate(1024);
				while (file_pos < file_size){
					size_t param_type;
					infile_ref.read(reinterpret_cast<char*>(&param_type), sizeof(param_type));
					file_pos += sizeof(param_type);
					infile_ref.read(reinterpret_cast<char*>(&entry.size_), sizeof(entry.size_));
					file_pos += sizeof(entry.size_);
					if (file_size - file_pos >= entry.size_){
						infile_ref.read(entry.char_ptr_, entry.size_);
						TxnParam* event_tuple = DeserializeParam(param_type, entry);
						if (event_tuple != NULL){
							input_batch.push_back(event_tuple);
						}
						file_pos += entry.size_;
					}
					else{
						break;
					}
				}
				entry.Release();
			}

			void ProcessLog(){
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

		private:
			ParamBatch *input_batches_;
		};
	}
}

#endif
