#if defined(LOCK_RTM)
#include "TransactionManager.h"

namespace Cavalia {
	namespace Database {
		bool TransactionManager::InsertRecord(TxnContext *context, const size_t &table_id, const std::string &primary_key, SchemaRecord *record) {
			BEGIN_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			// insert with visibility bit set to false.
			record->is_visible_ = false;
			TableRecord *tb_record = new TableRecord(record);
			// the to-be-inserted record may have already existed.
			//if (storage_manager_->tables_[table_id]->InsertRecord(primary_key, tb_record) == true){
			//if (tb_record->content_.TryWriteLock() == false){
			//	this->AbortTransaction();
			//	return false;
			//}
			tb_record->record_->is_visible_ = true;
			Access *access = hot_access_list_.NewAccess();
			access->access_type_ = INSERT_ONLY;
			access->access_record_ = tb_record;
			access->local_record_ = NULL;
			access->table_id_ = table_id;
			access->timestamp_ = 0;
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
			if (access_type == READ_ONLY) {
				auto &content_ref = t_record->content_;
				Access *access = NULL;
				if (content_ref.IsHot()){
					if (content_ref.TryReadLock() == false){
						this->AbortTransaction();
						return false;
					}
					access = hot_access_list_.NewAccess();
				}
				else{
					access = access_list_.NewAccess();
				}
				s_record = t_record->record_;
				access->access_type_ = READ_ONLY;
				access->access_record_ = t_record;
				access->local_record_ = NULL;
				access->table_id_ = table_id;
				content_ref.IncrementCounter();
				access->timestamp_ = content_ref.GetTimestamp();
				return true;
			}
			else if (access_type == READ_WRITE) {
				auto &content_ref = t_record->content_;
				Access *access = NULL;
				if (content_ref.IsHot()){
					if (content_ref.TryWriteLock() == false){
						this->AbortTransaction();
						return false;
					}
					access = hot_access_list_.NewAccess();
					// copy data
					BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
					const RecordSchema *schema_ptr = t_record->record_->schema_ptr_;
					char *local_data = MemAllocator::Alloc(schema_ptr->GetSchemaSize());
					SchemaRecord *local_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
					new(local_record)SchemaRecord(schema_ptr, local_data);
					END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
					local_record->CopyFrom(t_record->record_);
					content_ref.IncrementCounter();
					access->timestamp_ = content_ref.GetTimestamp();
					access->access_type_ = READ_WRITE;
					access->access_record_ = t_record;
					access->local_record_ = local_record;
					access->table_id_ = table_id;
					s_record = t_record->record_;
					return true;
				}
				else{
					access = access_list_.NewAccess();
					// copy data
					BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
					const RecordSchema *schema_ptr = t_record->record_->schema_ptr_;
					char *local_data = MemAllocator::Alloc(schema_ptr->GetSchemaSize());
					SchemaRecord *local_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
					new(local_record)SchemaRecord(schema_ptr, local_data);
					END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
					content_ref.IncrementCounter();
					access->timestamp_ = content_ref.GetTimestamp();
					COMPILER_MEMORY_FENCE;
					local_record->CopyFrom(t_record->record_);
					access->access_type_ = READ_WRITE;
					access->access_record_ = t_record;
					access->local_record_ = local_record;
					access->table_id_ = table_id;
					s_record = local_record;
					return true;
				}
			}
			// we just need to set the visibility bit on the record. so no need to create local copy.
			else if (access_type == DELETE_ONLY){
				auto &content_ref = t_record->content_;
				Access *access = NULL;
				if (content_ref.IsHot()){
					if (content_ref.TryWriteLock() == false){
						this->AbortTransaction();
						return false;
					}
					access = hot_access_list_.NewAccess();
				}
				else{
					access = access_list_.NewAccess();
				}
				s_record = t_record->record_;
				access->access_type_ = DELETE_ONLY;
				access->access_record_ = t_record;
				access->local_record_ = NULL;
				access->table_id_ = table_id;
				content_ref.IncrementCounter();
				access->timestamp_ = content_ref.GetTimestamp();
				return true;
			}
			else{
				assert(false);
				return true;
			}
		}

		bool TransactionManager::CommitTransaction(TxnContext *context, TxnParam *param, CharArray &ret_str){
			BEGIN_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			// acquire lock and validate
			bool is_success = true;
			// begin hardware transaction.
			if (access_list_.access_count_ != 0){
				rtm_lock_->Lock();
				for (size_t i = 0; i < access_list_.access_count_; ++i) {
					Access *access_ptr = access_list_.GetAccess(i);
					auto &content_ref = access_ptr->access_record_->content_;
					if (access_ptr->access_type_ == READ_ONLY) {
						// whether someone has changed the tuple after my read
						if (content_ref.GetTimestamp() != access_ptr->timestamp_) {
							UPDATE_CC_ABORT_COUNT(thread_id_, context->txn_type_, access_ptr->table_id_);
							is_success = false;
							break;
						}
					}
					else if (access_ptr->access_type_ == READ_WRITE) {
						// whether someone has changed the tuple after my read
						if (content_ref.GetTimestamp() != access_ptr->timestamp_) {
							UPDATE_CC_ABORT_COUNT(thread_id_, context->txn_type_, access_ptr->table_id_);
							is_success = false;
							break;
						}
					}
					else {
						// insert_only or delete_only
					}
				}
			}

			// if successful, then overwrite and commit
			//**********************************************************
			if (is_success == true) {
				BEGIN_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
				uint64_t curr_epoch = Epoch::GetEpoch();
#if defined(SCALABLE_TIMESTAMP)
				uint64_t max_rw_ts = 0;
				for (size_t i = 0; i < access_list_.access_count_; ++i){
					Access *access_ptr = access_list_.GetAccess(i);
					if (access_ptr->timestamp_ > max_rw_ts){
						max_rw_ts = access_ptr->timestamp_;
					}
				}
				for (size_t i = 0; i < hot_access_list_.access_count_; ++i){
					Access *access_ptr = hot_access_list_.GetAccess(i);
					if (access_ptr->timestamp_ > max_rw_ts){
						max_rw_ts = access_ptr->timestamp_;
					}
				}
				uint64_t commit_ts = GenerateScalableTimestamp(curr_epoch, max_rw_ts);
#else
				uint64_t commit_ts = GenerateMonotoneTimestamp(curr_epoch, GlobalTimestamp::GetMonotoneTimestamp());
#endif
				END_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
				if (access_list_.access_count_ != 0){
					for (size_t i = 0; i < access_list_.access_count_; ++i){
						Access *access_ptr = access_list_.GetAccess(i);
						SchemaRecord *global_record_ptr = access_ptr->access_record_->record_;
						SchemaRecord *local_record_ptr = access_ptr->local_record_;
						auto &content_ref = access_ptr->access_record_->content_;
						if (access_ptr->access_type_ == READ_WRITE){
							assert(commit_ts > access_ptr->timestamp_);
							std::swap(global_record_ptr, local_record_ptr);
							COMPILER_MEMORY_FENCE;
							content_ref.SetTimestamp(commit_ts);
						}
						else if (access_ptr->access_type_ == INSERT_ONLY){
							assert(commit_ts > access_ptr->timestamp_);
							global_record_ptr->is_visible_ = true;
							COMPILER_MEMORY_FENCE;
							content_ref.SetTimestamp(commit_ts);
						}
						else if (access_ptr->access_type_ == DELETE_ONLY){
							assert(commit_ts > access_ptr->timestamp_);
							global_record_ptr->is_visible_ = false;
							COMPILER_MEMORY_FENCE;
							content_ref.SetTimestamp(commit_ts);
						}
					}
					rtm_lock_->Unlock();
				}

				for (size_t i = 0; i < hot_access_list_.access_count_; ++i) {
					Access *access_ptr = hot_access_list_.GetAccess(i);
					auto &content_ref = access_ptr->access_record_->content_;
					if (access_ptr->access_type_ == READ_WRITE) {
						assert(commit_ts >= access_ptr->timestamp_);
						content_ref.SetTimestamp(commit_ts);
					}
					else if (access_ptr->access_type_ == INSERT_ONLY) {
						assert(commit_ts >= access_ptr->timestamp_);
						content_ref.SetTimestamp(commit_ts);
					}
					else if (access_ptr->access_type_ == DELETE_ONLY) {
						assert(commit_ts >= access_ptr->timestamp_);
						content_ref.SetTimestamp(commit_ts);
					}
				}

				// commit.
#if defined(VALUE_LOGGING)
				logger_->CommitTransaction(this->thread_id_, curr_epoch, commit_ts, access_list_);
#elif defined(COMMAND_LOGGING)
				if (context->is_adhoc_ == true){
					logger_->CommitTransaction(this->thread_id_, curr_epoch, commit_ts, access_list_);
				}
				logger_->CommitTransaction(this->thread_id_, curr_epoch, commit_ts, context->txn_type_, param);
#endif

				// release locks and clean up.
				for (size_t i = 0; i < hot_access_list_.access_count_; ++i) {
					Access *access_ptr = hot_access_list_.GetAccess(i);
					auto &content_ref = access_ptr->access_record_->content_;
					if (access_ptr->access_type_ == READ_ONLY) {
						content_ref.DecrementCounter();
						content_ref.ReleaseReadLock();
					}
					else if (access_ptr->access_type_ == READ_WRITE) {
						content_ref.DecrementCounter();
						content_ref.ReleaseWriteLock();
						BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
						SchemaRecord *local_record_ptr = access_ptr->local_record_;
						MemAllocator::Free(local_record_ptr->data_ptr_);
						local_record_ptr->~SchemaRecord();
						MemAllocator::Free((char*)local_record_ptr);
						END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
					}
					else{
						// insert_only or delete_only
						//content_ref.ReleaseWriteLock();
					}
				}

				// clean up accesses in cold access list.
				for (size_t i = 0; i < access_list_.access_count_; ++i) {
					Access *access_ptr = access_list_.GetAccess(i);
					auto &content_ref = access_ptr->access_record_->content_;
					if (access_ptr->access_type_ == READ_ONLY) {
						content_ref.DecrementCounter();
					}
					else if (access_ptr->access_type_ == READ_WRITE) {
						if (content_ref.DecrementCounter() == 0){
							BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
							SchemaRecord *local_record_ptr = access_ptr->local_record_;
							MemAllocator::Free(local_record_ptr->data_ptr_);
							local_record_ptr->~SchemaRecord();
							MemAllocator::Free((char*)local_record_ptr);
							END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
						}
						else{
							for (auto iter = garbage_set_.begin(); iter != garbage_set_.end();){
								if (iter->first->content_.GetCounter() == 0){
									BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
									SchemaRecord *local_record_ptr = iter->second;
									MemAllocator::Free(local_record_ptr->data_ptr_);
									local_record_ptr->~SchemaRecord();
									MemAllocator::Free((char*)local_record_ptr);
									END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
									iter = garbage_set_.erase(iter);
								}
								else{
									++iter;
								}
							}
							garbage_set_.push_back(std::make_pair(access_ptr->access_record_, access_ptr->local_record_));
						}
					}
				}
			}
			//**********************************************************
			// if failed.
			else {
				// end hardware transaction.
				if (access_list_.access_count_ != 0){
					rtm_lock_->Unlock();
					// clean up accesses in cold access list.
					for (size_t i = 0; i < access_list_.access_count_; ++i) {
						Access *access_ptr = access_list_.GetAccess(i);
						auto &content_ref = access_ptr->access_record_->content_;
						if (access_ptr->access_type_ == READ_ONLY) {
							content_ref.DecrementCounter();
						}
						if (access_ptr->access_type_ == READ_WRITE) {
							content_ref.DecrementCounter();
							BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
							SchemaRecord *local_record_ptr = access_ptr->local_record_;
							MemAllocator::Free(local_record_ptr->data_ptr_);
							local_record_ptr->~SchemaRecord();
							MemAllocator::Free((char*)local_record_ptr);
							END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
						}
						// inserts and deletes, wait for recycling to clean up
					}
				}
				// clean up accesses in hot access list.
				for (size_t i = 0; i < hot_access_list_.access_count_; ++i) {
					Access *access_ptr = hot_access_list_.GetAccess(i);
					SchemaRecord *global_record_ptr = access_ptr->access_record_->record_;
					SchemaRecord *local_record_ptr = access_ptr->local_record_;
					auto &content_ref = access_ptr->access_record_->content_;
					if (access_ptr->access_type_ == READ_ONLY) {
						content_ref.DecrementCounter();
						content_ref.ReleaseReadLock();
					}
					else if (access_ptr->access_type_ == READ_WRITE) {
						global_record_ptr->CopyFrom(local_record_ptr);
						content_ref.DecrementCounter();
						content_ref.ReleaseWriteLock();
						BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
						MemAllocator::Free(local_record_ptr->data_ptr_);
						local_record_ptr->~SchemaRecord();
						MemAllocator::Free((char*)local_record_ptr);
						END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
					}
					else if (access_ptr->access_type_ == INSERT_ONLY){
						global_record_ptr->is_visible_ = false;
						//content_ref.ReleaseWriteLock();
					}
					else if (access_ptr->access_type_ == DELETE_ONLY){
						global_record_ptr->is_visible_ = true;
						//content_ref.ReleaseWriteLock();
					}
				}
			}
			//**********************************************************
			assert(access_list_.access_count_ <= kMaxAccessNum);
			access_list_.Clear();
			assert(hot_access_list_.access_count_ <= kMaxAccessNum);
			hot_access_list_.Clear();
			END_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			return is_success;
		}

		void TransactionManager::AbortTransaction() {
			// clean up accesses in cold access list.
			for (size_t i = 0; i < access_list_.access_count_; ++i) {
				Access *access_ptr = access_list_.GetAccess(i);
				auto &content_ref = access_ptr->access_record_->content_;
				if (access_ptr->access_type_ == READ_ONLY) {
					content_ref.DecrementCounter();
				}
				if (access_ptr->access_type_ == READ_WRITE) {
					content_ref.DecrementCounter();
					BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
					SchemaRecord *local_record_ptr = access_ptr->local_record_;
					MemAllocator::Free(local_record_ptr->data_ptr_);
					local_record_ptr->~SchemaRecord();
					MemAllocator::Free((char*)local_record_ptr);
					END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
				}
				// inserts and deletes, wait for recycling to clean up
			}

			for (size_t i = 0; i < hot_access_list_.access_count_; ++i) {
				Access *access_ptr = hot_access_list_.GetAccess(i);
				SchemaRecord *global_record_ptr = access_ptr->access_record_->record_;
				SchemaRecord *local_record_ptr = access_ptr->local_record_;
				auto &content_ref = access_ptr->access_record_->content_;
				if (access_ptr->access_type_ == READ_ONLY) {
					content_ref.DecrementCounter();
					content_ref.ReleaseReadLock();
				}
				else if (access_ptr->access_type_ == READ_WRITE) {
					global_record_ptr->CopyFrom(local_record_ptr);
					content_ref.DecrementCounter();
					content_ref.ReleaseWriteLock();
					BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
					MemAllocator::Free(local_record_ptr->data_ptr_);
					local_record_ptr->~SchemaRecord();
					MemAllocator::Free((char*)local_record_ptr);
					END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
				}
				else if (access_ptr->access_type_ == INSERT_ONLY){
					global_record_ptr->is_visible_ = false;
					//content_ref.ReleaseWriteLock();
				}
				else if (access_ptr->access_type_ == DELETE_ONLY){
					global_record_ptr->is_visible_ = true;
					//content_ref.ReleaseWriteLock();
				}
			}
			assert(access_list_.access_count_ <= kMaxAccessNum);
			access_list_.Clear();
			assert(hot_access_list_.access_count_ <= kMaxAccessNum);
			hot_access_list_.Clear();
		}
	}
}

#endif
