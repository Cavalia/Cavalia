#if defined(LOCK_WAIT)
#include "TransactionManager.h"

namespace Cavalia{
	namespace StorageEngine{
		bool TransactionManager::InsertRecord(TxnContext *context, const size_t &table_id, const std::string &primary_key, SchemaRecord *record){
			BEGIN_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			if (is_first_access_ == true){
				BEGIN_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
#if defined(BATCH_TIMESTAMP)
				if (!batch_ts_.IsAvailable()){
					batch_ts_.InitTimestamp(GlobalContent::GetBatchMonotoneTimestamp());
				}
				start_timestamp_ = batch_ts_.GetTimestamp();
#else
				start_timestamp_ = GlobalContent::GetMonotoneTimestamp();
#endif
				is_first_access_ = false;
				END_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
			}
			Insertion *insertion = insertion_list_.NewInsertion();
			insertion->local_record_ = record;
			insertion->table_id_ = table_id;
			insertion->primary_key_ = primary_key;
			END_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			return true;
		}

		bool TransactionManager::SelectRecordCC(TxnContext *context, const size_t &table_id, TableRecord *t_record, SchemaRecord *&s_record, const AccessType access_type, const size_t &access_id, bool is_key_access) {
			s_record = t_record->record_;
			if (is_first_access_ == true){
				BEGIN_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
#if defined(BATCH_TIMESTAMP)
				if (!batch_ts_.IsAvailable()){
					batch_ts_.InitTimestamp(GlobalContent::GetBatchMonotoneTimestamp());
				}
				start_timestamp_ = batch_ts_.GetTimestamp();
#else
				start_timestamp_ = GlobalContent::GetMonotoneTimestamp();
#endif
				is_first_access_ = false;
				END_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
			}
			volatile bool lock_ready = true;
			if (access_type == READ_ONLY) {
				// if cannot get lock, then return immediately.
				if (t_record->content_.AcquireLock(start_timestamp_, LockType::READ_LOCK, &lock_ready) == false){
					this->AbortTransaction();
					return false;
				}

				BEGIN_CC_WAIT_TIME_MEASURE(thread_id_);
				while (lock_ready == false);
				END_CC_WAIT_TIME_MEASURE(thread_id_);

				Access *access = access_list_.NewAccess();
				access->access_type_ = READ_ONLY;
				access->access_record_ = t_record;
			}
			else if (access_type == READ_WRITE) {
				if (t_record->content_.AcquireLock(start_timestamp_, LockType::WRITE_LOCK, &lock_ready) == false) {
					this->AbortTransaction();
					return false;
				}

				BEGIN_CC_WAIT_TIME_MEASURE(thread_id_);
				while (lock_ready == false);
				END_CC_WAIT_TIME_MEASURE(thread_id_);

				char *local_data = allocator_->Alloc(t_record->record_->schema_ptr_->GetSchemaSize());
				SchemaRecord *local_record = (SchemaRecord*)allocator_->Alloc(sizeof(SchemaRecord));
				new(local_record)SchemaRecord(t_record->record_->schema_ptr_, local_data);
				t_record->record_->CopyTo(local_record);
				Access *access = access_list_.NewAccess();
				access->access_type_ = READ_WRITE;
				access->access_record_ = t_record;
				access->local_record_ = local_record;
			}
			else{
				assert(access_type == DELETE_ONLY);
				if (t_record->content_.AcquireLock(start_timestamp_, LockType::WRITE_LOCK, &lock_ready) == false) {
					this->AbortTransaction();
					return false;
				}
				else{
					t_record->record_->is_visible_ = false;
					Access *access = access_list_.NewAccess();
					access->access_type_ = DELETE_ONLY;
					access->access_record_ = t_record;
				}
			}
			return true;
		}

		bool TransactionManager::CommitTransaction(TxnContext *context, EventTuple *param, CharArray &ret_str){
			BEGIN_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			// install.
			for (size_t i = 0; i < insertion_list_.insertion_count_; ++i) {
				Insertion *insertion_ptr = insertion_list_.GetInsertion(i);
				TableRecord *tb_record = new TableRecord(insertion_ptr->local_record_);
				volatile bool lock_ready = true;
				bool rc = tb_record->content_.AcquireLock(start_timestamp_, LockType::WRITE_LOCK, &lock_ready);
				assert(rc == true && lock_ready == true);
				insertion_ptr->insertion_record_ = tb_record;
				//storage_manager_->tables_[insertion_ptr->table_id_]->InsertRecord(insertion_ptr->primary_key_, insertion_ptr->insertion_record_);
			}
			// commit.
			//logger_->CommitTransaction(txn_type, param);
			// release locks.
			for (size_t i = 0; i < insertion_list_.insertion_count_; ++i) {
				Insertion *insertion_ptr = insertion_list_.GetInsertion(i);
				insertion_ptr->insertion_record_->content_.ReleaseLock(start_timestamp_);
			}
			for (size_t i = 0; i < access_list_.access_count_; ++i){
				Access *access_ptr = access_list_.GetAccess(i);
				if (access_ptr->access_type_ == READ_ONLY){
					access_ptr->access_record_->content_.ReleaseLock(start_timestamp_);
				}
				else if (access_ptr->access_type_ == READ_WRITE){
					access_ptr->access_record_->content_.ReleaseLock(start_timestamp_);
					allocator_->Free(access_ptr->local_record_->data_ptr_);
					access_ptr->local_record_->~SchemaRecord();
					allocator_->Free((char*)access_ptr->local_record_);
				}
				else{
					assert(access_ptr->access_type_ == DELETE_ONLY);
					access_ptr->access_record_->content_.ReleaseLock(start_timestamp_);
				}
			}
			assert(insertion_list_.insertion_count_ <= kMaxAccessNum);
			assert(access_list_.access_count_ <= kMaxAccessNum);
			insertion_list_.Clear();
			access_list_.Clear();
			is_first_access_ = true;
			END_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			return true;
		}

		void TransactionManager::AbortTransaction() {
			// recover updated data and release locks.
			for (size_t i = 0; i < insertion_list_.insertion_count_; ++i) {
				Insertion *insertion_ptr = insertion_list_.GetInsertion(i);
				allocator_->Free(insertion_ptr->local_record_->data_ptr_);
				insertion_ptr->local_record_->~SchemaRecord();
				allocator_->Free((char*)insertion_ptr->local_record_);
			}

			for (size_t i = 0; i < access_list_.access_count_; ++i){
				Access *access_ptr = access_list_.GetAccess(i);
				if (access_ptr->access_type_ == READ_ONLY){
					access_ptr->access_record_->content_.ReleaseLock(start_timestamp_);
				}
				else if (access_ptr->access_type_ == READ_WRITE){
					access_ptr->access_record_->record_->CopyFrom(access_ptr->local_record_);
					access_ptr->access_record_->content_.ReleaseLock(start_timestamp_);
					allocator_->Free(access_ptr->local_record_->data_ptr_);
					access_ptr->local_record_->~SchemaRecord();
					allocator_->Free((char*)access_ptr->local_record_);
				}
				else{
					assert(access_ptr->access_type_ == DELETE_ONLY);
					access_ptr->access_record_->record_->is_visible_ = true;
					access_ptr->access_record_->content_.ReleaseLock(start_timestamp_);
				}
			}
			assert(insertion_list_.insertion_count_ <= kMaxAccessNum);
			assert(access_list_.access_count_ <= kMaxAccessNum);
			insertion_list_.Clear();
			access_list_.Clear();
		}
	}
}

#endif
