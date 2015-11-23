#if defined(SILOCK)
#include "TransactionManager.h"

namespace Cavalia{
	namespace StorageEngine{
		bool TransactionManager::InsertRecord(TxnContext *context, const size_t &table_id, const std::string &primary_key, SchemaRecord *record){
			BEGIN_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			if (is_first_access_ == true){
				start_timestamp_ = GlobalContent::GetMaxTimestamp();
				is_first_access_ = false;
			}
			//storage_manager_->tables_[table_id]->InsertRecord(record);
			Access *access = access_list_.NewAccess();
			access->access_type_ = INSERT_ONLY;
			access->access_record_ = NULL;
			access->local_record_ = record;
			access->table_id_ = table_id;
			access->primary_key_ = primary_key;
			END_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			return true;
		}

		bool TransactionManager::SelectRecordCC(TxnContext *context, const size_t &table_id, const std::string &primary_key, SchemaRecord *&record, const AccessType access_type) {
			if (is_first_access_ == true) {
				start_timestamp_ = GlobalContent::GetMaxTimestamp();
				is_first_access_ = false;
			}
			// acquire write lock.
			if (access_type == READ_WRITE){
				if (record->content_.AcquireWriteLock() == false){
					this->AbortTransaction();
					return false;
				}
				else if (record->content_.Validate(start_timestamp_) == false){
					record->content_.ReleaseWriteLock();
					this->AbortTransaction();
					return false;
				}
			}
			char* tmp_data = NULL;
			// read never abort or block
			record->content_.ReadAccess(start_timestamp_, tmp_data);
			Access *access = access_list_.NewAccess();
			access->access_type_ = access_type;
			access->access_record_ = record;
			SchemaRecord *local_record = (SchemaRecord*)allocator_->Alloc(sizeof(SchemaRecord));
			if (access_type == READ_ONLY) {
				// directly return the versioned copy.
				new(local_record)SchemaRecord(record->schema_ptr_, tmp_data);
			}
			else {
				// write in local copy
				size_t size = record->schema_ptr_->GetSchemaSize();
				char* local_data = allocator_->Alloc(size);
				memcpy(local_data, tmp_data, size);
				new(local_record)SchemaRecord(record->schema_ptr_, local_data);
			}
			access->local_record_ = local_record;
			record = local_record;
			return true;
		}

		bool TransactionManager::CommitTransaction(TxnContext *context, EventTuple *param){
			BEGIN_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			int64_t commit_timestamp = GlobalContent::GetMonotoneTimestamp();
			//install writes
			for (size_t i = 0; i < access_list_.access_count_; ++i){
				Access *access_ptr = access_list_.GetAccess(i);
				if (access_ptr->access_type_ == READ_WRITE){
					// update
					access_ptr->access_record_->content_.WriteAccess(commit_timestamp, access_ptr->local_record_->data_ptr_);
				}
				else if (access_ptr->access_type_ == INSERT_ONLY){
					// insert
					access_ptr->local_record_->content_.SetTimestamp(commit_timestamp);
					storage_manager_->tables_[access_ptr->table_id_]->InsertRecord(access_ptr->primary_key_, access_ptr->local_record_);
				}
			}

			//release lock
			for (size_t i = 0; i < access_list_.access_count_; ++i){
				Access *access_ptr = access_list_.GetAccess(i);
				if (access_ptr->access_type_ == READ_WRITE){
					access_ptr->access_record_->content_.ReleaseWriteLock();
				}
			}
			// cleanup
			for (size_t i = 0; i < access_list_.access_count_; ++i){
				Access *access_ptr = access_list_.GetAccess(i);
				if (access_ptr->access_type_ != INSERT_ONLY){
					// data_ptr in local records of both read and write should be alive
					access_ptr->local_record_->data_ptr_ = NULL;
					access_ptr->local_record_->~SchemaRecord();
					allocator_->Free((char*)access_ptr->local_record_);
				}
			}
			assert(access_list_.access_count_ <= kMaxAccessNum);
			access_list_.Clear();
			GlobalContent::SetThreadTimestamp(thread_id_, commit_timestamp);
			is_first_access_ = true;
			END_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			return true;
		}

		void TransactionManager::AbortTransaction() {
			for (size_t i = 0; i < access_list_.access_count_; ++i){
				Access *access_ptr = access_list_.GetAccess(i);
				if (access_ptr->access_type_ == READ_ONLY){
					access_ptr->local_record_->data_ptr_ = NULL;
				}
				else if (access_ptr->access_type_ == READ_WRITE){
					access_ptr->access_record_->content_.ReleaseWriteLock();
					allocator_->Free(access_ptr->local_record_->data_ptr_);
				}
				else{
					allocator_->Free(access_ptr->local_record_->data_ptr_);
				}
				access_ptr->local_record_->~SchemaRecord();
				allocator_->Free((char*)access_ptr->local_record_);
			}
			assert(access_list_.access_count_ <= kMaxAccessNum);
			access_list_.Clear();
			is_first_access_ = true;
		}
	}
}

#endif