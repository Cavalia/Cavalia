#if defined(MVTO)
#include "TransactionManager.h"

namespace Cavalia{
	namespace Database{
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
			if (context->is_read_only_ == true){
				if (is_first_access_ == true){
					BEGIN_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
					start_timestamp_ = GlobalContent::GetMinTimestamp();
					is_first_access_ = false;
					END_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
				}
				volatile bool is_ready = false;
				char* tmp_data = NULL;
				t_record->content_.RequestReadAccess(start_timestamp_, &tmp_data, &is_ready);
				BEGIN_CC_WAIT_TIME_MEASURE(thread_id_);
				while (is_ready == false);
				END_CC_WAIT_TIME_MEASURE(thread_id_);
				assert(tmp_data != NULL);
				SchemaRecord* local_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
				new(local_record)SchemaRecord(t_record->record_->schema_ptr_, tmp_data);
				read_only_set_.push_back(local_record);
				s_record = local_record;
				return true;
			}
			////////////////////////////////////////////

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
			// issue write request first
			if (access_type == READ_WRITE){
				if (t_record->content_.RequestWriteAccess(start_timestamp_, NULL) == false){
					UPDATE_CC_ABORT_COUNT(thread_id_, 0, table_id);
					this->AbortTransaction();
					return false;
				}
			}
			volatile bool is_ready = false;
			char* tmp_data = NULL;
			t_record->content_.RequestReadAccess(start_timestamp_, &tmp_data, &is_ready);
			BEGIN_CC_WAIT_TIME_MEASURE(thread_id_);
			while (is_ready == false);
			END_CC_WAIT_TIME_MEASURE(thread_id_);
			assert(tmp_data != NULL);

			Access *access = access_list_.NewAccess();
			access->access_type_ = access_type;
			access->access_record_ = t_record;

			if (access_type == READ_ONLY){
				SchemaRecord* local_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
				new(local_record)SchemaRecord(t_record->record_->schema_ptr_, tmp_data);
				s_record = local_record;
				access->local_record_ = local_record;
			}
			else{
				assert(access_type == READ_WRITE);
				size_t sz = t_record->record_->schema_ptr_->GetSchemaSize();
				char* local_data = MemAllocator::Alloc(sz);
				memcpy(local_data, tmp_data, sz);
				SchemaRecord* local_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
				new(local_record)SchemaRecord(t_record->record_->schema_ptr_, local_data);
				s_record = local_record;
				access->local_record_ = local_record;
			}
			return true;
		}

		bool TransactionManager::CommitTransaction(TxnContext *context, TxnParam *param, CharArray &ret_str){
			BEGIN_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			if (context->is_read_only_ == true){
				for (auto &entry : read_only_set_){
					MemAllocator::Free((char*)entry);
				}
				read_only_set_.clear();
				is_first_access_ = true;
				END_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
				return true;
			}
			////////////////////////////////////////////

			for (size_t i = 0; i < access_list_.access_count_; ++i){
				Access *access_ptr = access_list_.GetAccess(i);
				if (access_ptr->access_type_ == READ_WRITE){
					assert(access_ptr->access_record_ != NULL);
					//access_ptr->access_record_->content_.RequestCommit(start_timestamp_);
					access_ptr->access_record_->content_.RequestCommit(start_timestamp_, access_ptr->local_record_->data_ptr_);
					access_ptr->local_record_->data_ptr_ = NULL;
					access_ptr->local_record_->~SchemaRecord();
					MemAllocator::Free((char*)access_ptr->local_record_);
				}
				else if (access_ptr->access_type_ == READ_ONLY){
					access_ptr->local_record_->data_ptr_ = NULL;
					access_ptr->local_record_->~SchemaRecord();
					MemAllocator::Free((char*)access_ptr->local_record_);
				}
				//else{
				//	assert(access_ptr->access_type_ == INSERT_ONLY);
				//	storage_manager_->tables_[access_ptr->table_id_]->InsertRecord(access_ptr->primary_key_, access_ptr->local_record_);
				//}
			}
			assert(insertion_list_.insertion_count_ <= kMaxAccessNum);
			assert(access_list_.access_count_ <= kMaxAccessNum);
			insertion_list_.Clear();
			access_list_.Clear();

			//logger_->CommitTransaction(txn_type, param);
			GlobalContent::SetThreadTimestamp(thread_id_, start_timestamp_);
			is_first_access_ = true;
			END_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			return true;
		}

		void TransactionManager::AbortTransaction() {
			for (size_t i = 0; i < access_list_.access_count_; ++i){
				Access *access_ptr = access_list_.GetAccess(i);
				// not insert
				if (access_ptr->access_type_ == READ_ONLY) {
					access_ptr->local_record_->data_ptr_ = NULL;
				}
				else if (access_ptr->access_type_ == READ_WRITE){
					access_ptr->access_record_->content_.RequestAbort(start_timestamp_);
					MemAllocator::Free(access_ptr->local_record_->data_ptr_);
				}
				else{
					MemAllocator::Free(access_ptr->local_record_->data_ptr_);
				}
				access_ptr->local_record_->~SchemaRecord();
				MemAllocator::Free((char*)access_ptr->local_record_);
			}
			assert(access_list_.access_count_ <= kMaxAccessNum);
			access_list_.Clear();
			GlobalContent::SetThreadTimestamp(thread_id_, start_timestamp_);
			is_first_access_ = true;
		}
	}
}

#endif
