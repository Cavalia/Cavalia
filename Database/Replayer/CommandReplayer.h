#pragma once
#ifndef __CAVALIA_DATABASE_COMMAND_REPLAYER_H__
#define __CAVALIA_DATABASE_COMMAND_REPLAYER_H__

#include <unordered_map>
#include "../Transaction/TransactionManager.h"
#include "../Transaction/StoredProcedure.h"
#include "../Transaction/TxnParam.h"
#include "BaseReplayer.h"

namespace Cavalia{
	namespace Database{
		class CommandReplayer : public BaseReplayer{
		public:
			CommandReplayer(const std::string &filename, BaseStorageManager *const storage_manager, const size_t &thread_count) : BaseReplayer(filename, storage_manager, thread_count, false){
				input_batches_ = new VariableParamBatch[thread_count_];
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
					procedures[entry.first] = entry.second();
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
				FILE *infile_ptr = infiles_[thread_id];
				VariableParamBatch &input_batch = input_batches_[thread_id];
				fseek(infile_ptr, 0L, SEEK_END);
				size_t file_size = ftell(infile_ptr);
				rewind(infile_ptr);
				size_t file_pos = 0;
				CharArray entry;
				entry.Allocate(1024);
				int result = 0;
				while (file_pos < file_size){
					uint64_t timestamp;
					result = fread(&timestamp, sizeof(timestamp), 1, infile_ptr);
					assert(result == 1);
					file_pos += sizeof(timestamp);
					size_t param_type;
					result = fread(&param_type, sizeof(param_type), 1, infile_ptr);
					assert(result == 1);
					file_pos += sizeof(param_type);
					result = fread(&entry.size_, sizeof(entry.size_), 1, infile_ptr);
					assert(result == 1);
					file_pos += sizeof(entry.size_);
					result = fread(entry.char_ptr_, 1, entry.size_, infile_ptr);
					assert(result == entry.size_);
					TxnParam* event_tuple = DeserializeParam(param_type, entry);
					if (event_tuple != NULL){
						input_batch.push_back(event_tuple);
					}
					file_pos += entry.size_;
				}
				assert(file_pos == file_size);
				entry.Release();
			}

			void ProcessLog(){
				for (size_t i = 0; i < thread_count_; ++i){
					std::cout << "thread id=" << i << ", size=" << input_batches_[i].size() << std::endl;
				}
				//std::string ret;
				//for (auto &log_pair : log_buffer_){
				//	procedures_[log_pair.first]->Execute(log_pair.second, ret);
				//}
			}

		private:
			CommandReplayer(const CommandReplayer &);
			CommandReplayer& operator=(const CommandReplayer &);

		protected:
			std::unordered_map<size_t, std::function<StoredProcedure*()>> registers_;

		private:
			VariableParamBatch *input_batches_;
		};
	}
}

#endif
