#if defined(MVLOCK_WAIT)

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
			//if (tb_record->content_.TryWriteLock() == false){
			//	this->AbortTransaction();
			//	return false;
			//}
			tb_record->record_->is_visible_ = true;
			Access *access = access_list_.NewAccess();
			access->access_type_ = INSERT_ONLY;
			access->access_record_ = tb_record;
			access->local_record_ = NULL;
			access->table_id_ = table_id;
			END_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			return true;
			/*}
			else{
			// if the record has already existed, then we need to lock the original record.
			END_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			return true;
			}*/
		}

		bool TransactionManager::SelectRecordCC(TxnContext *context, const size_t &table_id, TableRecord *t_record, SchemaRecord *&s_record, const AccessType access_type) {
			if (context->is_read_only_ == true){
				if (is_first_access_ == true){
					BEGIN_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
					start_timestamp_ = GlobalTimestamp::GetMaxTimestamp();
					END_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
					is_first_access_ = false;
				}
				char *tmp_data = NULL;
				t_record->content_.ReadAccess(start_timestamp_, tmp_data);
				SchemaRecord *local_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
				new(local_record)SchemaRecord(t_record->record_->schema_ptr_, tmp_data);
				read_only_set_.push_back(local_record);
				s_record = local_record;
				return true;
			}

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
			volatile bool lock_ready = true;
			if (access_type == READ_ONLY) {
				// if cannot get lock, then return immediately.
				if (t_record->content_.AcquireLock(start_timestamp_, LockType::READ_LOCK, &lock_ready) == false) {
					this->AbortTransaction();
					return false;
				}
				else {
					BEGIN_CC_WAIT_TIME_MEASURE(thread_id_);
					while (lock_ready == false);
					END_CC_WAIT_TIME_MEASURE(thread_id_);

					// return latest version.
					char *tmp_data = NULL;
					t_record->content_.ReadAccess(tmp_data);
					SchemaRecord *local_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
					new(local_record)SchemaRecord(t_record->record_->schema_ptr_, tmp_data);
					Access *access = access_list_.NewAccess();
					access->access_type_ = READ_ONLY;
					access->access_record_ = t_record;
					access->local_record_ = local_record;
					access->table_id_ = table_id;
					s_record = local_record;
				}
			}
			else if (access_type == READ_WRITE){
				if (t_record->content_.AcquireLock(start_timestamp_, LockType::WRITE_LOCK, &lock_ready) == false) {
					this->AbortTransaction();
					return false;
				}
				else {
					BEGIN_CC_WAIT_TIME_MEASURE(thread_id_);
					while (lock_ready == false);
					END_CC_WAIT_TIME_MEASURE(thread_id_);

					// return latest version.
					char *tmp_data = NULL;
					t_record->content_.ReadAccess(tmp_data);
					const RecordSchema *schema_ptr = t_record->record_->schema_ptr_;
					size_t size = schema_ptr->GetSchemaSize();
					char *local_data = MemAllocator::Alloc(size);
					memcpy(local_data, tmp_data, size);
					SchemaRecord *local_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
					new(local_record)SchemaRecord(schema_ptr, local_data);
					Access *access = access_list_.NewAccess();
					access->access_type_ = READ_WRITE;
					access->access_record_ = t_record;
					access->local_record_ = local_record;
					access->table_id_ = table_id;
					s_record = local_record;
				}
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

			bool is_success = true;
			// count number of certify locks.
			size_t certify_count = 0;
			// upgrade write lock to certify lock.
			for (size_t i = 0; i < access_list_.access_count_; ++i){
				Access *access_ptr = access_list_.GetAccess(i);
				if (access_ptr->access_type_ == READ_WRITE){
					// try to upgrade to certify lock.
					volatile bool lock_ready = true;
					if (access_ptr->access_record_->content_.AcquireLock(start_timestamp_, LockType::CERTIFY_LOCK, &lock_ready) == false){
						is_success = false;
						break;
					}
					else{
						BEGIN_CC_WAIT_TIME_MEASURE(thread_id_);
						while (lock_ready == false);
						END_CC_WAIT_TIME_MEASURE(thread_id_);
						++certify_count;
					}
				}
			}
			// install.
			uint64_t commit_timestamp = 0;
			if (is_success == true){
				commit_timestamp = GlobalTimestamp::GetMonotoneTimestamp();
				for (size_t i = 0; i < access_list_.access_count_; ++i){
					Access *access_ptr = access_list_.GetAccess(i);
					if (access_ptr->access_type_ == READ_WRITE){
						// install from local copy.
						access_ptr->access_record_->content_.WriteAccess(commit_timestamp, access_ptr->local_record_->data_ptr_);
					}
				}
			}

			// release locks.
			for (size_t i = 0; i < access_list_.access_count_; ++i){
				Access *access_ptr = access_list_.GetAccess(i);
				if (access_ptr->access_type_ == READ_ONLY) {
					access_ptr->access_record_->content_.ReleaseLock(start_timestamp_);
				}
				else if (access_ptr->access_type_ == READ_WRITE) {
					if (certify_count > 0){
						access_ptr->access_record_->content_.ReleaseLock(start_timestamp_);
						--certify_count;
					}
					else{
						access_ptr->access_record_->content_.ReleaseLock(start_timestamp_);
					}
				}
			}
			assert(certify_count == 0);

			if (is_success == true){
				// clean up.
				for (size_t i = 0; i < access_list_.access_count_; ++i){
					Access *access_ptr = access_list_.GetAccess(i);
					if (access_ptr->access_type_ == READ_ONLY || access_ptr->access_type_ == READ_WRITE){
						SchemaRecord *local_record_ptr = access_ptr->local_record_;
						local_record_ptr->data_ptr_ = NULL;
						local_record_ptr->~SchemaRecord();
						MemAllocator::Free((char*)local_record_ptr);
					}
				}
				GlobalTimestamp::SetThreadTimestamp(thread_id_, commit_timestamp);
			}
			else{
				for (size_t i = 0; i < access_list_.access_count_; ++i){
					Access *access_ptr = access_list_.GetAccess(i);
					if (access_ptr->access_type_ == READ_ONLY){
						SchemaRecord *local_record_ptr = access_ptr->local_record_;
						local_record_ptr->data_ptr_ = NULL;
						local_record_ptr->~SchemaRecord();
						MemAllocator::Free((char*)local_record_ptr);
					}
					else if (access_ptr->access_type_ == READ_WRITE){
						SchemaRecord *local_record_ptr = access_ptr->local_record_;
						MemAllocator::Free(local_record_ptr->data_ptr_);
						local_record_ptr->~SchemaRecord();
						MemAllocator::Free((char*)local_record_ptr);
					}
					else{

					}
				}
			}
			assert(access_list_.access_count_ <= kMaxAccessNum);
			access_list_.Clear();
			is_first_access_ = true;
			END_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			return is_success;
		}

		void TransactionManager::AbortTransaction() {
			for (size_t i = 0; i < access_list_.access_count_; ++i){
				Access *access_ptr = access_list_.GetAccess(i);
				if (access_ptr->access_type_ == READ_ONLY){
					access_ptr->access_record_->content_.ReleaseLock(start_timestamp_);
					SchemaRecord *local_record_ptr = access_ptr->local_record_;
					local_record_ptr->data_ptr_ = NULL;
					local_record_ptr->~SchemaRecord();
					MemAllocator::Free((char*)local_record_ptr);
				}
				else if (access_ptr->access_type_ == READ_WRITE){
					access_ptr->access_record_->content_.ReleaseLock(start_timestamp_);
					SchemaRecord *local_record_ptr = access_ptr->local_record_;
					MemAllocator::Free(local_record_ptr->data_ptr_);
					local_record_ptr->~SchemaRecord();
					MemAllocator::Free((char*)local_record_ptr);
				}
			}
			// stil use original timestamp if abort
			assert(access_list_.access_count_ <= kMaxAccessNum);
			access_list_.Clear();
		}
	}
}
#endif
