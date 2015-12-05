#if defined(LOCK)
#include <iostream>
#include "TransactionManager.h"

namespace Cavalia{
	namespace Database{
		bool TransactionManager::InsertRecord(TxnContext *context, const size_t &table_id, const std::string &primary_key, SchemaRecord *record){
			BEGIN_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			record->is_visible_ = false;
			TableRecord *tb_record = new TableRecord(record);
			// upsert.
			//if (storage_manager_->tables_[table_id]->InsertRecord(primary_key, tb_record) == true){
				if (tb_record->content_.TryWriteLock() == false){
					this->AbortTransaction();
					return false;
				}
				tb_record->record_->is_visible_ = true;
				Access *access = access_list_.NewAccess();
				access->access_type_ = INSERT_ONLY;
				access->access_record_ = tb_record;
				access->local_record_ = NULL;
				access->table_id_ = table_id;
				access->timestamp_ = 0;
				END_PHASE_MEASURE(thread_id_, INSERT_PHASE);
				return true;
			/*}
			else{
				//assert(false);
				END_PHASE_MEASURE(thread_id_, INSERT_PHASE);
				return true;
			}*/
		}

		bool TransactionManager::SelectRecordCC(TxnContext *context, const size_t &table_id, TableRecord *t_record, SchemaRecord *&s_record, const AccessType access_type) {
			s_record = t_record->record_;
			if (access_type == READ_ONLY) {
				// if cannot get lock, then return immediately.
				if (t_record->content_.TryReadLock() == false) {
					this->AbortTransaction();
					return false;
				}
				else{
					Access *access = access_list_.NewAccess();
					access->access_type_ = READ_ONLY;
					access->access_record_ = t_record;
					access->local_record_ = NULL;
					access->table_id_ = table_id;
					access->timestamp_ = t_record->content_.GetTimestamp();
					return true;
				}
			}
			else if (access_type == READ_WRITE) {
				if (t_record->content_.TryWriteLock() == false) {
					this->AbortTransaction();
					return false;
				}
				else {
					const RecordSchema *schema_ptr = t_record->record_->schema_ptr_;
					char *local_data = MemAllocator::Alloc(schema_ptr->GetSchemaSize());
					SchemaRecord *local_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
					new(local_record)SchemaRecord(schema_ptr, local_data);
					t_record->record_->CopyTo(local_record);
					Access *access = access_list_.NewAccess();
					access->access_type_ = READ_WRITE;
					access->access_record_ = t_record;
					access->local_record_ = local_record;
					access->table_id_ = table_id;
					access->timestamp_ = t_record->content_.GetTimestamp();
					return true;
				}
			}
			else if (access_type == DELETE_ONLY){
				if (t_record->content_.TryWriteLock() == false){
					this->AbortTransaction();
					return false;
				}
				else{
					t_record->record_->is_visible_ = false;
					Access *access = access_list_.NewAccess();
					access->access_type_ = DELETE_ONLY;
					access->access_record_ = t_record;
					access->local_record_ = NULL;
					access->table_id_ = table_id;
					access->timestamp_ = t_record->content_.GetTimestamp();
					return true;
				}
			}
			else{
				assert(false);
				return true;
			}
		}

		bool TransactionManager::CommitTransaction(TxnContext *context, TxnParam *param, CharArray &ret_str){
			BEGIN_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			uint64_t max_rw_ts = 0;
			for (size_t i = 0; i < access_list_.access_count_; ++i){
				Access *access_ptr = access_list_.GetAccess(i);
				if (access_ptr->timestamp_ > max_rw_ts){
					max_rw_ts = access_ptr->timestamp_;
				}
			}

			BEGIN_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
			uint64_t global_ts = ScalableTimestamp::GetTimestamp();
			END_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
			uint64_t commit_ts = GenerateTimestamp(global_ts, max_rw_ts);
			assert(commit_ts >= max_rw_ts);

			for (size_t i = 0; i < access_list_.access_count_; ++i){
				Access *access_ptr = access_list_.GetAccess(i);
				TableRecord *access_record = access_ptr->access_record_;
				if (access_ptr->access_type_ == READ_WRITE){
					assert(commit_ts >= access_ptr->timestamp_);
					access_record->content_.SetTimestamp(commit_ts);
#if defined(VALUE_LOGGING)
					((ValueLogger*)logger_)->UpdateRecord(this->thread_id_, access_ptr->table_id_, access_ptr->local_record_->data_ptr_, access_record->record_->schema_ptr_->GetSchemaSize());
#endif
				}
				else if (access_ptr->access_type_ == INSERT_ONLY){
					assert(commit_ts >= access_ptr->timestamp_);
					access_record->content_.SetTimestamp(commit_ts);
#if defined(VALUE_LOGGING)
					((ValueLogger*)logger_)->InsertRecord(this->thread_id_, access_ptr->table_id_, access_record->record_->data_ptr_, access_record->record_->schema_ptr_->GetSchemaSize());
#endif
				}
				else if (access_ptr->access_type_ == DELETE_ONLY){
					assert(commit_ts >= access_ptr->timestamp_);
					access_record->content_.SetTimestamp(commit_ts);
#if defined(VALUE_LOGGING)
					((ValueLogger*)logger_)->DeleteRecord(this->thread_id_, access_ptr->table_id_, access_record->record_->GetPrimaryKey());
#endif
				}
			}
			// commit.
#if defined(VALUE_LOGGING)
			((ValueLogger*)logger_)->CommitTransaction(this->thread_id_, global_ts, commit_ts);
#elif defined(COMMAND_LOGGING)
			((CommandLogger*)logger_)->CommitTransaction(this->thread_id_, global_ts, context->txn_type_, param);
#endif
			// release locks.
			for (size_t i = 0; i < access_list_.access_count_; ++i){
				Access *access_ptr = access_list_.GetAccess(i);
				if (access_ptr->access_type_ == READ_ONLY){
					access_ptr->access_record_->content_.ReleaseReadLock();
				}
				else if (access_ptr->access_type_ == READ_WRITE){
					access_ptr->access_record_->content_.ReleaseWriteLock();
					MemAllocator::Free(access_ptr->local_record_->data_ptr_);
					access_ptr->local_record_->~SchemaRecord();
					MemAllocator::Free((char*)access_ptr->local_record_);
				}
				else {
					// insert_only or delete_only
					access_ptr->access_record_->content_.ReleaseWriteLock();
				}
			}
			assert(access_list_.access_count_ <= kMaxAccessNum);
			access_list_.Clear();
			END_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			return true;
		}

		void TransactionManager::AbortTransaction() {
			// recover updated data and release locks.
			for (size_t i = 0; i < access_list_.access_count_; ++i){
				Access *access_ptr = access_list_.GetAccess(i);
				if (access_ptr->access_type_ == READ_ONLY){
					access_ptr->access_record_->content_.ReleaseReadLock();
				}
				else if (access_ptr->access_type_ == READ_WRITE){
					access_ptr->access_record_->record_->CopyFrom(access_ptr->local_record_);
					access_ptr->access_record_->content_.ReleaseWriteLock();
					MemAllocator::Free(access_ptr->local_record_->data_ptr_);
					access_ptr->local_record_->~SchemaRecord();
					MemAllocator::Free((char*)access_ptr->local_record_);
				}
				else if (access_ptr->access_type_ == INSERT_ONLY){
					access_ptr->access_record_->record_->is_visible_ = false;
					access_ptr->access_record_->content_.ReleaseWriteLock();
				}
				else if (access_ptr->access_type_ == DELETE_ONLY){
					access_ptr->access_record_->record_->is_visible_ = true;
					access_ptr->access_record_->content_.ReleaseWriteLock();
				}
			}
			assert(access_list_.access_count_ <= kMaxAccessNum);
			access_list_.Clear();
		}
	}
}

#endif
