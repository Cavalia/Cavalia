#if defined(HYBRID)
#include "TransactionManager.h"

namespace Cavalia{
	namespace Database{
		bool TransactionManager::InsertRecord(TxnContext *context, const size_t &table_id, const std::string &primary_key, SchemaRecord *record){
			BEGIN_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			if (context->is_retry_ == false){
				record->is_visible_ = false;
				TableRecord *tb_record = new TableRecord(record);
				// upsert.
				storage_manager_->tables_[table_id]->InsertRecord(primary_key, tb_record);
				Access *access = access_list_.NewAccess();
				access->access_type_ = INSERT_ONLY;
				access->access_record_ = tb_record;
				access->local_record_ = NULL;
			}
			else{
				record->is_visible_ = false;
				TableRecord *tb_record = new TableRecord(record);
				// upsert.
				storage_manager_->tables_[table_id]->InsertRecord(primary_key, tb_record);
				if (tb_record->content_.TryWriteLock() == false){
					this->AbortTransaction();
					return false;
				}
				tb_record->record_->is_visible_ = true;
				Access *access = access_list_.NewAccess();
				access->access_type_ = INSERT_ONLY;
				access->local_record_ = NULL;
			}
			END_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			return true;
		}

		bool TransactionManager::SelectRecordCC(TxnContext *context, const size_t &table_id, TableRecord *t_record, SchemaRecord *&s_record, const AccessType access_type, const size_t &access_id, bool is_key_access) {
			if (context->is_retry_ == false){
				if (access_type == READ_ONLY) {
					Access *access = access_list_.NewAccess();
					access->access_type_ = READ_ONLY;
					access->access_record_ = t_record;
					access->local_record_ = NULL;
					access->timestamp_ = t_record->content_.GetTimestamp();
					s_record = t_record->record_;
					return true;
				}
				else if (access_type == READ_WRITE) {
					Access *access = access_list_.NewAccess();
					access->access_type_ = READ_WRITE;
					access->access_record_ = t_record;
					// copy data
					BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
					const RecordSchema *schema_ptr = t_record->record_->schema_ptr_;
					char *local_data = MemAllocator::Alloc(schema_ptr->GetSchemaSize());
					SchemaRecord *local_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
					new(local_record)SchemaRecord(schema_ptr, local_data);
					END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
					access->timestamp_ = t_record->content_.GetTimestamp();
					COMPILER_MEMORY_FENCE;
					local_record->CopyFrom(t_record->record_);
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
					access->local_record_ = NULL;
					s_record = t_record->record_;
					return true;
				}
			}
			else{
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
						access->timestamp_ = t_record->content_.GetTimestamp();
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
						access->timestamp_ = t_record->content_.GetTimestamp();
					}
				}
				else {
					assert(access_type == DELETE_ONLY);
					if (t_record->content_.TryWriteLock() == false){
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
		}

		bool TransactionManager::CommitTransaction(TxnContext *context, TxnParam *param, CharArray &ret_str){
			if (context->is_retry_ == false){
				BEGIN_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
				// step 1: acquire lock and validate
				uint64_t max_rw_ts = 0;
				size_t lock_count = 0;
				bool is_success = true;
				access_list_.Sort();
				for (size_t i = 0; i < access_list_.access_count_; ++i) {
					++lock_count;
					Access *access_ptr = access_list_.GetAccess(i);
					if (access_ptr->access_type_ == READ_ONLY) {
						// acquire read lock
						access_ptr->access_record_->content_.AcquireReadLock();
						// whether someone has changed the tuple after my read
						if (access_ptr->access_record_->content_.GetTimestamp() != access_ptr->timestamp_) {
							UPDATE_CC_ABORT_COUNT(thread_id_, context->txn_type_, access_ptr->access_record_->GetTableId());
							is_success = false;
							break;
						}
						if (access_ptr->timestamp_ > max_rw_ts){
							max_rw_ts = access_ptr->timestamp_;
						}
					}
					else if (access_ptr->access_type_ == READ_WRITE) {
						// acquire write lock
						access_ptr->access_record_->content_.AcquireWriteLock();
						// whether someone has changed the tuple after my read
						if (access_ptr->access_record_->content_.GetTimestamp() != access_ptr->timestamp_) {
							UPDATE_CC_ABORT_COUNT(thread_id_, context->txn_type_, access_ptr->access_record_->GetTableId());
							is_success = false;
							break;
						}
						if (access_ptr->timestamp_ > max_rw_ts){
							max_rw_ts = access_ptr->timestamp_;
						}
					}
					else {
						// write_only, insert_only, or delete_only
						access_ptr->access_record_->content_.AcquireWriteLock();
					}
				}
				// step 2: if success, then overwrite and commit
				if (is_success == true) {
					BEGIN_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
					uint64_t curr_ts = ScalableTimestamp::GetTimestamp();
					END_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
					uint64_t commit_ts = GenerateTimestamp(curr_ts, max_rw_ts);

					for (size_t i = 0; i < access_list_.access_count_; ++i) {
						Access *access_ptr = access_list_.GetAccess(i);
						TableRecord *access_record = access_ptr->access_record_;
						if (access_ptr->access_type_ == READ_WRITE) {
							assert(commit_ts > access_ptr->timestamp_);
							access_record->record_->CopyFrom(access_ptr->local_record_);
							COMPILER_MEMORY_FENCE;
							access_record->content_.SetTimestamp(commit_ts);
						}
						else if (access_ptr->access_type_ == INSERT_ONLY) {
							assert(commit_ts > access_ptr->timestamp_);
							access_record->record_->is_visible_ = true;
							COMPILER_MEMORY_FENCE;
							access_record->content_.SetTimestamp(commit_ts);
						}
						else if (access_ptr->access_type_ == DELETE_ONLY) {
							assert(commit_ts > access_ptr->timestamp_);
							access_record->record_->is_visible_ = false;
							COMPILER_MEMORY_FENCE;
							access_record->content_.SetTimestamp(commit_ts);
						}
					}
					// commit.
					// step 3: release locks and clean up.
					for (size_t i = 0; i < access_list_.access_count_; ++i) {
						Access *access_ptr = access_list_.GetAccess(i);
						if (access_ptr->access_type_ == READ_ONLY) {
							access_ptr->access_record_->content_.ReleaseReadLock();
						}
						else if (access_ptr->access_type_ == READ_WRITE) {
							access_ptr->access_record_->content_.ReleaseWriteLock();
							BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
							MemAllocator::Free(access_ptr->local_record_->data_ptr_);
							access_ptr->local_record_->~SchemaRecord();
							MemAllocator::Free((char*)access_ptr->local_record_);
							END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
						}
						else{
							access_ptr->access_record_->content_.ReleaseWriteLock();
						}
					}
				}
				// if failed.
				else {
					// step 3: release locks and clean up.
					for (size_t i = 0; i < access_list_.access_count_; ++i) {
						Access *access_ptr = access_list_.GetAccess(i);
						if (access_ptr->access_type_ == READ_ONLY) {
							access_ptr->access_record_->content_.ReleaseReadLock();
						}
						else if (access_ptr->access_type_ == READ_WRITE) {
							access_ptr->access_record_->content_.ReleaseWriteLock();
							BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
							MemAllocator::Free(access_ptr->local_record_->data_ptr_);
							access_ptr->local_record_->~SchemaRecord();
							MemAllocator::Free((char*)access_ptr->local_record_);
							END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
						}
						else {
							access_ptr->access_record_->content_.ReleaseWriteLock();
						}
						--lock_count;
						if (lock_count == 0) {
							break;
						}
					}
				}
				assert(access_list_.access_count_ <= kMaxAccessNum);
				access_list_.Clear();
				END_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
				return is_success;
			}
			else{
				BEGIN_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
				uint64_t max_rw_ts = 0;
				for (size_t i = 0; i < access_list_.access_count_; ++i){
					Access *access_ptr = access_list_.GetAccess(i);
					if (access_ptr->timestamp_ > max_rw_ts){
						max_rw_ts = access_ptr->timestamp_;
					}
				}

				BEGIN_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
				uint64_t curr_ts = ScalableTimestamp::GetTimestamp();
				END_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
				uint64_t commit_ts = GenerateTimestamp(curr_ts, max_rw_ts);

				for (size_t i = 0; i < access_list_.access_count_; ++i){
					Access *access_ptr = access_list_.GetAccess(i);
					if (access_ptr->access_type_ == READ_WRITE){
						assert(commit_ts >= access_ptr->timestamp_);
						access_ptr->access_record_->content_.SetTimestamp(commit_ts);
					}
					else if (access_ptr->access_type_ == INSERT_ONLY){
						assert(commit_ts >= access_ptr->timestamp_);
						access_ptr->access_record_->content_.SetTimestamp(commit_ts);
					}
					else if (access_ptr->access_type_ == DELETE_ONLY){
						assert(commit_ts >= access_ptr->timestamp_);
						access_ptr->access_record_->content_.SetTimestamp(commit_ts);
					}
				}
				// commit.
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
					else if (access_ptr->access_type_ == INSERT_ONLY){
						access_ptr->access_record_->content_.ReleaseWriteLock();
					}
					else if (access_ptr->access_type_ == DELETE_ONLY){
						access_ptr->access_record_->content_.ReleaseWriteLock();
					}
				}
				assert(access_list_.access_count_ <= kMaxAccessNum);
				access_list_.Clear();
				END_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
				return true;
			}
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
				else{
					assert(access_ptr->access_type_ == DELETE_ONLY);
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
