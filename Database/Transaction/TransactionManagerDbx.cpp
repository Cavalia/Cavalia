#if defined(DBX)
#include "TransactionManager.h"

namespace Cavalia{
	namespace Database{
		bool TransactionManager::InsertRecord(TxnContext *context, const size_t &table_id, const std::string &primary_key, SchemaRecord *record) {
			BEGIN_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			record->is_visible_ = false;
			TableRecord *tb_record = new TableRecord(record);
			// upsert.
			storage_manager_->tables_[table_id]->InsertRecord(primary_key, tb_record);
			Access *access = access_list_.NewAccess();
			access->access_type_ = INSERT_ONLY;
			access->access_record_ = tb_record;
			access->local_record_ = NULL;
			access->table_id_ = table_id;
			access->timestamp_ = 0;
			END_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			return true;
		}

		bool TransactionManager::SelectRecordCC(TxnContext *context, const size_t &table_id, TableRecord *t_record, SchemaRecord *&s_record, const AccessType access_type, const size_t &access_id, bool is_key_access) {
			if (access_type == READ_ONLY) {
				Access *access = access_list_.NewAccess();
				access->access_type_ = READ_ONLY;
				access->access_record_ = t_record;
				// ensure consistent view of timestamp_ and record_
				rtm_lock_->Lock();
				access->timestamp_ = t_record->timestamp_;
				s_record = t_record->record_;
				rtm_lock_->Unlock();
				return true;
			}
			else if (access_type == READ_WRITE) {
				Access *access = access_list_.NewAccess();
				access->access_type_ = READ_WRITE;
				access->access_record_ = t_record;
				// ensure consistent view of timestamp_ and record_
				rtm_lock_->Lock();
				access->timestamp_ = t_record->timestamp_;
				SchemaRecord* tmp_record = t_record->record_;
				rtm_lock_->Unlock();
				// copy data
				BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
				char *local_data = MemAllocator::Alloc(tmp_record->schema_ptr_->GetSchemaSize());
				SchemaRecord *local_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
				new(local_record)SchemaRecord(tmp_record->schema_ptr_, local_data);
				END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
				local_record->CopyFrom(tmp_record);
				access->local_record_ = local_record;
				// reset returned record.
				s_record = local_record;
				return true;
			}
			else {
				assert(access_type == DELETE_ONLY);
				Access *access = access_list_.NewAccess();
				access->access_type_ = DELETE_ONLY;
				access->access_record_ = t_record;
				s_record = t_record->record_;
				return true;
			}
		}

		bool TransactionManager::CommitTransaction(TxnContext *context, TxnParam *param, CharArray &ret_str){
			BEGIN_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			// step 1: acquire lock and validate
			bool is_success = true;
			
			// begin hardware transaction.
			rtm_lock_->Lock();
			for (size_t i = 0; i < access_list_.access_count_; ++i) {
				Access *access_ptr = access_list_.GetAccess(i);
				if (access_ptr->access_type_ == READ_ONLY) {
					// whether someone has changed the tuple after my read
					if (access_ptr->access_record_->timestamp_ != access_ptr->timestamp_) {
						UPDATE_CC_ABORT_COUNT(thread_id_, context->txn_type_, access_ptr->access_record_->GetTableId());
						is_success = false;
						break;
					}

				}
				else if (access_ptr->access_type_ == READ_WRITE) {
					// whether someone has changed the tuple after my read
					if (access_ptr->access_record_->timestamp_ != access_ptr->timestamp_ || access_ptr->access_record_->record_->is_visible_ == false) {
						UPDATE_CC_ABORT_COUNT(thread_id_, context->txn_type_, access_ptr->access_record_->GetTableId());
						is_success = false;
						break;
					}
				}
			}
			// step 2: if success, then overwrite and commit
			if (is_success == true) {
				// get global epoch id for commit
				BEGIN_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
				uint64_t global_ts = ScalableTimestamp::GetTimestamp();
				END_CC_TS_ALLOC_TIME_MEASURE(thread_id_);

				for (size_t i = 0; i < access_list_.access_count_; ++i) {
					Access *access_ptr = access_list_.GetAccess(i);
					TableRecord *access_record = access_ptr->access_record_;
					if (access_ptr->access_type_ == READ_WRITE) {
						// exchanging pointers, the old version would be recycled
						std::swap(access_record->record_, access_ptr->local_record_);
						access_record->timestamp_++;
					}
					else if (access_ptr->access_type_ == INSERT_ONLY){
						access_ptr->access_record_->record_->is_visible_ = true;
					}
					else if (access_ptr->access_type_ == DELETE_ONLY) {
						access_record->record_->is_visible_ = false;
						// update timestamp to invalidate concurrent reads
						access_record->timestamp_++;
					}
				}
				// end hardware transaction.
				rtm_lock_->Unlock();

				// logging, outside rtm region
#if defined(VALUE_LOGGING)
				for (size_t i = 0; i < access_list_.access_count_; ++i){
					Access* access_ptr = access_list_.GetAccess(i);
					TableRecord *access_record = access_ptr->access_record_;
					if (access_ptr->access_type_ == READ_WRITE){
						((ValueLogger*)logger_)->UpdateRecord(this->thread_id_, access_ptr->table_id_, access_record->record_->data_ptr_, access_record->record_->schema_ptr_->GetSchemaSize());
					}
					else if (access_ptr->access_type_ == INSERT_ONLY){
						((ValueLogger*)logger_)->InsertRecord(this->thread_id_, access_ptr->table_id_, access_ptr->local_record_->data_ptr_, access_record->record_->schema_ptr_->GetSchemaSize());
					}
					else if (access_ptr->access_type_ == DELETE_ONLY){
						((ValueLogger*)logger_)->DeleteRecord(this->thread_id_, access_ptr->table_id_, access_record->record_->GetPrimaryKey());
					}
				}
				((ValueLogger*)logger_)->CommitTransaction(this->thread_id_, global_ts, 0);
#elif defined(COMMAND_LOGGING)
				((CommandLogger*)logger_)->CommitTransaction(this->thread_id_, global_ts, context->txn_type_, param);
#endif
				// clean up.
				for (size_t i = 0; i < access_list_.access_count_; ++i) {
					Access *access_ptr = access_list_.GetAccess(i);
					if (access_ptr->access_type_ == READ_WRITE) {
						BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
						MemAllocator::Free(access_ptr->local_record_->data_ptr_);
						access_ptr->local_record_->~SchemaRecord();
						MemAllocator::Free((char*)access_ptr->local_record_);
						END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
					}
					// deletes, wait for recycling to clean up
				}
			}
			// if failed.
			else {
				// end hardware transaction.
				rtm_lock_->Unlock();
				// clean up 
				for (size_t i = 0; i < access_list_.access_count_; ++i) {
					Access *access_ptr = access_list_.GetAccess(i);
					if (access_ptr->access_type_ == READ_WRITE) {
						BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
						MemAllocator::Free(access_ptr->local_record_->data_ptr_);
						access_ptr->local_record_->~SchemaRecord();
						MemAllocator::Free((char*)access_ptr->local_record_);
						END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
					}
					// inserts and deletes, wait for recycling to clean up
				}
			}
			assert(access_list_.access_count_ <= kMaxAccessNum);
			access_list_.Clear();
			END_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			return is_success;
		}

		void TransactionManager::AbortTransaction() {
			assert(false);
		}
	}
}

#endif
