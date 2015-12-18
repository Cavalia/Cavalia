#pragma once
#ifndef __CAVALIA_DATABASE_SERIAL_COMMAND_REPLAYER_H__
#define __CAVALIA_DATABASE_SERIAL_COMMAND_REPLAYER_H__

#include <unordered_map>
#include "BaseCommandReplayer.h"

namespace Cavalia{
	namespace Database{
		class SerialCommandReplayer : public BaseCommandReplayer{
		public:
			SerialCommandReplayer(const std::string &filename, BaseStorageManager *const storage_manager, const size_t &thread_count) : BaseCommandReplayer(filename, storage_manager, thread_count){}
			virtual ~SerialCommandReplayer(){}

		private:
			virtual void ProcessLog(){
				PrepareProcedures();
				TransactionManager *txn_manager = new TransactionManager(storage_manager_, NULL);
				StoredProcedure **procedures = new StoredProcedure*[registers_.size()];
				for (auto &entry : registers_){
					procedures[entry.first] = entry.second();
					procedures[entry.first]->SetTransactionManager(txn_manager);
				}
				CharArray ret;
				ret.char_ptr_ = new char[1024];
				ExeContext exe_context;
				for (size_t k = 0; k < serial_log_sequences_.size(); ++k){
					for (auto &log_entry : *(serial_log_sequences_.at(k).second)){
						if (log_entry->is_command_log_ == true){
							TxnParam *param = (static_cast<CommandLogEntry*>(log_entry))->param_;
							ret.size_ = 0;
							procedures[param->type_]->Execute(param, ret, exe_context);
						}
						else{
							for (size_t k = 0; k < (static_cast<ValueLogEntry*>(log_entry))->element_count_; ++k){
								ValueLogElement *log_element_ptr = &((static_cast<ValueLogEntry*>(log_entry))->elements_[k]);
								if (log_element_ptr->type_ == kInsert){
									SchemaRecord *record_ptr = new SchemaRecord(GetRecordSchema(log_element_ptr->table_id_), log_element_ptr->data_ptr_);
									//storage_manager_->tables_[log_element_ptr->table_id_]->InsertRecord(new TableRecord(record_ptr));
								}
								else if (log_element_ptr->type_ == kUpdate){
									SchemaRecord *record_ptr = new SchemaRecord(GetRecordSchema(log_element_ptr->table_id_), log_element_ptr->data_ptr_);
									TableRecord *tb_record_ptr = NULL;
									storage_manager_->tables_[log_element_ptr->table_id_]->SelectKeyRecord(record_ptr->GetPrimaryKey(), tb_record_ptr);
									delete tb_record_ptr->record_;
									tb_record_ptr->record_ = record_ptr;
								}
								else if (log_element_ptr->type_ == kDelete){
									//storage_manager_->tables_[log_element_ptr->table_id_]->DeleteRecord();
								}
							}
						}
					}
				}
			}

			virtual void PrepareProcedures() = 0;

		protected:
			std::unordered_map<size_t, std::function<StoredProcedure*()>> registers_;

		private:
			SerialCommandReplayer(const SerialCommandReplayer &);
			SerialCommandReplayer& operator=(const SerialCommandReplayer &);
		};
	}
}

#endif
