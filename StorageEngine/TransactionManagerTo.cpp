#if defined(TO)
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
			// we assume that concurrent txns will not read this insert record.
			Insertion *insertion = insertion_list_.NewInsertion();
			insertion->local_record_ = record;
			insertion->table_id_ = table_id;
			insertion->primary_key_ = primary_key;
			END_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			return true;
		}

		bool TransactionManager::SelectRecordCC(TxnContext *context, const size_t &table_id, TableRecord *t_record, SchemaRecord *&s_record, const AccessType access_type, const size_t &access_id, bool is_key_access) {
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
			char *local_data = allocator_->Alloc(t_record->record_->schema_ptr_->GetSchemaSize());
			SchemaRecord *local_record = (SchemaRecord*)allocator_->Alloc(sizeof(SchemaRecord));
			new(local_record)SchemaRecord(t_record->record_->schema_ptr_, local_data);
			// local_record should be allocated here.
			if (access_type == READ_WRITE) {
				// write will be pushed into a queue without block. 
				// write should be installed right before commit.
				if (t_record->content_.RequestWriteAccess(start_timestamp_, &local_record->data_ptr_) == false) {
					UPDATE_CC_ABORT_COUNT(thread_id_, context->txn_type_, table_id);
					this->AbortTransaction();
					return false;
				}
			}
			volatile bool is_ready = false;
			// local data may be allocated in this function.
			if (t_record->content_.RequestReadAccess(start_timestamp_, &local_record->data_ptr_, &is_ready) == false) {
				// local record should be reclaimed here.
				allocator_->Free(local_data);
				local_record->~SchemaRecord();
				allocator_->Free((char*)local_record);
				UPDATE_CC_ABORT_COUNT(thread_id_, context->txn_type_, table_id);
				this->AbortTransaction();
				return false;
			}
			if (is_ready == false){
				BEGIN_CC_WAIT_TIME_MEASURE(thread_id_);
				UPDATE_CC_WAIT_COUNT(thread_id_, context->txn_type_, table_id);
				while (is_ready == false);
				END_CC_WAIT_TIME_MEASURE(thread_id_);
			}
			// here, local data must have already been allocated.
			Access *access = access_list_.NewAccess();
			access->access_type_ = access_type;
			access->access_record_ = t_record;
			access->local_record_ = local_record;
			// reset returned record.
			s_record = local_record;
			return true;
		}

		bool TransactionManager::CommitTransaction(TxnContext *context, EventTuple *param, CharArray &ret_str){
			BEGIN_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			for (size_t i = 0; i < access_list_.access_count_; ++i) {
				Access *access_ptr = access_list_.GetAccess(i);
				// if is write, then install it.
				if (access_ptr->access_type_ == READ_WRITE){
					volatile bool is_ready = false;
					access_ptr->access_record_->content_.RequestCommit(start_timestamp_, &is_ready);
					allocator_->Free(access_ptr->local_record_->data_ptr_);
					access_ptr->local_record_->~SchemaRecord();
					allocator_->Free((char*)access_ptr->local_record_);
				}
				else if (access_ptr->access_type_ == READ_ONLY){
					allocator_->Free(access_ptr->local_record_->data_ptr_);
					access_ptr->local_record_->~SchemaRecord();
					allocator_->Free((char*)access_ptr->local_record_);
				}
				//else {
				//	assert(access_ptr->access_type_ == INSERT_ONLY);
				//	storage_manager_->tables_[access_ptr->table_id_]->InsertRecord(access_ptr->primary_key_, access_ptr->local_record_);
				//}
			}
			assert(insertion_list_.insertion_count_ <= kMaxAccessNum);
			assert(access_list_.access_count_ <= kMaxAccessNum);
			insertion_list_.Clear();
			access_list_.Clear();
			// when to commit??
			is_first_access_ = true;
			END_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			return true;
		}

		void TransactionManager::AbortTransaction() {
			for (size_t i = 0; i < access_list_.access_count_; ++i) {
				Access *access_ptr = access_list_.GetAccess(i);
				if (access_ptr->access_type_ == READ_WRITE) {
					access_ptr->access_record_->content_.RequestAbort(start_timestamp_);
				}
				allocator_->Free(access_ptr->local_record_->data_ptr_);
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
