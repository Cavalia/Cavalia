#if defined(TO)
#include "TransactionManager.h"

namespace Cavalia{
	namespace Database{
		bool TransactionManager::InsertRecord(TxnContext *context, const size_t &table_id, const std::string &primary_key, SchemaRecord *record){
			BEGIN_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			if (is_first_access_ == true){
				BEGIN_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
#if defined(BATCH_TIMESTAMP)
				if (!batch_ts_.IsAvailable()){
					batch_ts_.InitTimestamp(GlobalTimestamp::GetBatchMonotoneTimestamp());
				}
				start_timestamp_ = batch_ts_.GetTimestamp();
#else
				start_timestamp_ = GlobalTimestamp::GetMonotoneTimestamp();
#endif
				is_first_access_ = false;
				END_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
			}
			record->is_visible_ = false;
			TableRecord *tb_record = new TableRecord(record);
			//if (storage_manager_->tables_[table_id]->InsertRecord(primary_key, tb_record) == true){
				tb_record->record_->is_visible_ = true;
				Access *access = access_list_.NewAccess();
				access->access_type_ = INSERT_ONLY;
				access->access_record_ = tb_record;
				access->local_record_ = NULL;
				access->table_id_ = table_id;
				END_PHASE_MEASURE(thread_id_, INSERT_PHASE);
				return true;
			//}
			//else{
			//	// if the record has already existed, then we need to lock the original record.
			//	END_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			//	return true;
			//}
		}

		bool TransactionManager::SelectRecordCC(TxnContext *context, const size_t &table_id, TableRecord *t_record, SchemaRecord *&s_record, const AccessType access_type) {
			if (is_first_access_ == true){
				BEGIN_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
#if defined(BATCH_TIMESTAMP)
				if (!batch_ts_.IsAvailable()){
					batch_ts_.InitTimestamp(GlobalTimestamp::GetBatchMonotoneTimestamp());
				}
				start_timestamp_ = batch_ts_.GetTimestamp();
#else
				start_timestamp_ = GlobalTimestamp::GetMonotoneTimestamp();
#endif
				is_first_access_ = false;
				END_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
			}
			const RecordSchema *schema_ptr = t_record->record_->schema_ptr_;
			// local_record should be allocated here.
			char *local_data = MemAllocator::Alloc(schema_ptr->GetSchemaSize());
			SchemaRecord *local_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
			new(local_record)SchemaRecord(schema_ptr, local_data);
			if (access_type == READ_WRITE) {
				// write will be pushed into a queue without blocking.
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
				MemAllocator::Free(local_data);
				local_record->~SchemaRecord();
				MemAllocator::Free((char*)local_record);
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
			access->table_id_ = table_id;
			// reset returned record.
			s_record = local_record;
			return true;
		}

		bool TransactionManager::CommitTransaction(TxnContext *context, TxnParam *param, CharArray &ret_str){
			BEGIN_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			for (size_t i = 0; i < access_list_.access_count_; ++i) {
				Access *access_ptr = access_list_.GetAccess(i);
				// if it is write, then install it.
				if (access_ptr->access_type_ == READ_WRITE){
					volatile bool is_ready = false;
					access_ptr->access_record_->content_.RequestCommit(start_timestamp_, &is_ready);
					MemAllocator::Free(access_ptr->local_record_->data_ptr_);
					access_ptr->local_record_->~SchemaRecord();
					MemAllocator::Free((char*)access_ptr->local_record_);
				}
				else if (access_ptr->access_type_ == READ_ONLY){
					MemAllocator::Free(access_ptr->local_record_->data_ptr_);
					access_ptr->local_record_->~SchemaRecord();
					MemAllocator::Free((char*)access_ptr->local_record_);
				}
			}
			assert(access_list_.access_count_ <= kMaxAccessNum);
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
				MemAllocator::Free(access_ptr->local_record_->data_ptr_);
				access_ptr->local_record_->~SchemaRecord();
				MemAllocator::Free((char*)access_ptr->local_record_);
			}
			assert(access_list_.access_count_ <= kMaxAccessNum);
			access_list_.Clear();
			is_first_access_ = true;
		}
	}
}

#endif
