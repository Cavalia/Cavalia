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
					s_record = local_record;
				}
			}
			else {
				assert(access_type == READ_WRITE);
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
					size_t size = t_record->record_->schema_ptr_->GetSchemaSize();
					char *local_data = MemAllocator::Alloc(size);
					memcpy(local_data, tmp_data, size);
					SchemaRecord *local_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
					new(local_record)SchemaRecord(t_record->record_->schema_ptr_, local_data);
					Access *access = access_list_.NewAccess();
					access->access_type_ = READ_WRITE;
					access->access_record_ = t_record;
					access->local_record_ = local_record;
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
			int64_t commit_timestamp = 0;
			if (is_success == true){
				commit_timestamp = GlobalTimestamp::GetMonotoneTimestamp();
				for (size_t i = 0; i < access_list_.access_count_; ++i){
					Access *access_ptr = access_list_.GetAccess(i);
					if (access_ptr->access_type_ == READ_WRITE){
						// install from local copy.
						access_ptr->access_record_->content_.WriteAccess(commit_timestamp, access_ptr->local_record_->data_ptr_);
					}
					//else if (access_ptr->access_type_ == INSERT_ONLY){
					//	// install from local copy.
					//	storage_manager_->tables_[access_ptr->table_id_]->InsertRecord(access_ptr->primary_key_, access_ptr->local_record_);
					//}
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
					access_ptr->local_record_->data_ptr_ = NULL;
					access_ptr->local_record_->~SchemaRecord();
					MemAllocator::Free((char*)access_ptr->local_record_);
				}
				assert(access_list_.access_count_ <= kMaxAccessNum);
				access_list_.Clear();
				GlobalTimestamp::SetThreadTimestamp(thread_id_, commit_timestamp);
			}
			else{
				for (size_t i = 0; i < access_list_.access_count_; ++i){
					Access *access_ptr = access_list_.GetAccess(i);
					if (access_ptr->access_type_ == READ_ONLY){
						access_ptr->local_record_->data_ptr_ = NULL;
					}
					else{
						MemAllocator::Free(access_ptr->local_record_->data_ptr_);
					}
					access_ptr->local_record_->~SchemaRecord();
					MemAllocator::Free((char*)access_ptr->local_record_);
				}
				assert(access_list_.access_count_ <= kMaxAccessNum);
				access_list_.Clear();
			}
			is_first_access_ = true;
			// for mvlock, no need to set is_first_access.
			END_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			return is_success;
		}

		void TransactionManager::AbortTransaction() {
			for (size_t i = 0; i < access_list_.access_count_; ++i){
				Access *access_ptr = access_list_.GetAccess(i);
				if (access_ptr->access_type_ == READ_ONLY){
					access_ptr->access_record_->content_.ReleaseLock(start_timestamp_);
					access_ptr->local_record_->data_ptr_ = NULL;
				}
				else if (access_ptr->access_type_ == READ_WRITE){
					access_ptr->access_record_->content_.ReleaseLock(start_timestamp_);
					MemAllocator::Free(access_ptr->local_record_->data_ptr_);
				}
				//else{
				//	assert(access_ptr->access_type_ == INSERT_ONLY);
				//	MemAllocator::Free(access_ptr->local_record_->data_ptr_);
				//}
				access_ptr->local_record_->~SchemaRecord();
				MemAllocator::Free((char*)access_ptr->local_record_);
			}
			// stil use original timestamp if abort
			assert(access_list_.access_count_ <= kMaxAccessNum);
			access_list_.Clear();
		}
	}
}
#endif
