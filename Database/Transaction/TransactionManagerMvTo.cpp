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
					batch_ts_.InitTimestamp(GlobalTimestamp::GetBatchMonotoneTimestamp());
				}
				start_timestamp_ = batch_ts_.GetTimestamp();
#else
				start_timestamp_ = GlobalTimestamp::GetMonotoneTimestamp();
#endif
				is_first_access_ = false;
				END_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
			}
			// we assume that concurrent txns will not read this insert record.
			record->is_visible_ = false;
			TableRecord *tb_record = new TableRecord(record);
			// upsert.
			storage_manager_->tables_[table_id]->InsertRecord(primary_key, tb_record);
			Access *access = access_list_.NewAccess();
			access->access_type_ = INSERT_ONLY;
			access->access_record_ = tb_record;
			access->local_record_ = NULL;
			access->table_id_ = table_id;
			END_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			return true;
		}

		bool TransactionManager::SelectRecordCC(TxnContext *context, const size_t &table_id, TableRecord *t_record, SchemaRecord *&s_record, const AccessType access_type) {
			if (context->is_read_only_ == true){
				if (is_first_access_ == true){
					BEGIN_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
					start_timestamp_ = GlobalTimestamp::GetMinTimestamp();
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
					batch_ts_.InitTimestamp(GlobalTimestamp::GetBatchMonotoneTimestamp());
				}
				start_timestamp_ = batch_ts_.GetTimestamp();
#else
				start_timestamp_ = GlobalTimestamp::GetMonotoneTimestamp();
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


			if (access_type == READ_ONLY){
				Access *access = access_list_.NewAccess();
				access->access_type_ = access_type;
				access->access_record_ = t_record;
				access->table_id_ = table_id;
				SchemaRecord* local_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
				new(local_record)SchemaRecord(t_record->record_->schema_ptr_, tmp_data);
				access->local_record_ = local_record;
				s_record = local_record;
				return true;
			}
			else if (access_type == READ_WRITE){
				Access *access = access_list_.NewAccess();
				access->access_type_ = access_type;
				access->access_record_ = t_record;
				access->table_id_ = table_id;
				const RecordSchema *schema_ptr = t_record->record_->schema_ptr_;
				size_t size = schema_ptr->GetSchemaSize();
				char* local_data = MemAllocator::Alloc(size);
				memcpy(local_data, tmp_data, size);
				SchemaRecord* local_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
				new(local_record)SchemaRecord(schema_ptr, local_data);
				access->local_record_ = local_record;
				s_record = local_record;
				return true;
			}
			else{
				assert(false);
				return true;
			}
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
			}
			assert(access_list_.access_count_ <= kMaxAccessNum);
			access_list_.Clear();

			//logger_->CommitTransaction(txn_type, param);
			GlobalTimestamp::SetThreadTimestamp(thread_id_, start_timestamp_);
			is_first_access_ = true;
			END_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			return true;
		}

		void TransactionManager::AbortTransaction() {
			for (size_t i = 0; i < access_list_.access_count_; ++i){
				Access *access_ptr = access_list_.GetAccess(i);
				// not insert
				if (access_ptr->access_type_ == READ_ONLY) {
					SchemaRecord *local_record_ptr = access_ptr->local_record_;
					local_record_ptr->data_ptr_ = NULL;
					local_record_ptr->~SchemaRecord();
					MemAllocator::Free((char*)local_record_ptr);
				}
				else if (access_ptr->access_type_ == READ_WRITE){
					access_ptr->access_record_->content_.RequestAbort(start_timestamp_);
					SchemaRecord *local_record_ptr = access_ptr->local_record_;
					MemAllocator::Free(local_record_ptr->data_ptr_);
					local_record_ptr->~SchemaRecord();
					MemAllocator::Free((char*)local_record_ptr);
				}
				else{
				}
			}
			assert(access_list_.access_count_ <= kMaxAccessNum);
			access_list_.Clear();
			GlobalTimestamp::SetThreadTimestamp(thread_id_, start_timestamp_);
			is_first_access_ = true;
		}
	}
}

#endif
