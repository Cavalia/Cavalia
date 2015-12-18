#if defined(OCC)
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
				Access *access = access_list_.NewAccess();
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
				Access *access = access_list_.NewAccess();
				access->access_type_ = READ_ONLY;
				access->access_record_ = t_record;
				access->local_record_ = NULL;
				access->table_id_ = table_id;
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
				access->table_id_ = table_id;
				// reset returned record.
				s_record = local_record;
				return true;
			}
			// we just need to set the visibility bit on the record. so no need to create local copy.
			else if (access_type == DELETE_ONLY){
				Access *access = access_list_.NewAccess();
				access->access_type_ = DELETE_ONLY;
				access->access_record_ = t_record;
				access->local_record_ = NULL;
				access->table_id_ = table_id;
				access->timestamp_ = t_record->content_.GetTimestamp();
				s_record = t_record->record_;
				return true;
			}
			else{
				assert(false);
				return true;
			}
		}

		bool TransactionManager::CommitTransaction(TxnContext *context, TxnParam *param, CharArray &ret_str){
			BEGIN_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			// step 1: acquire lock and validate
			size_t lock_count = 0;
			bool is_success = true;
			access_list_.Sort();
			for (size_t i = 0; i < access_list_.access_count_; ++i) {
				++lock_count;
				Access *access_ptr = access_list_.GetAccess(i);
				auto &content_ref = access_ptr->access_record_->content_;
				if (access_ptr->access_type_ == READ_ONLY) {
					// acquire read lock
					content_ref.AcquireReadLock();
					// whether someone has changed the tuple after my read
					if (content_ref.GetTimestamp() != access_ptr->timestamp_) {
						UPDATE_CC_ABORT_COUNT(thread_id_, context->txn_type_, access_ptr->table_id_);
						is_success = false;
						break;
					}
				}
				else if (access_ptr->access_type_ == READ_WRITE) {
					// acquire write lock
					content_ref.AcquireWriteLock();
					// whether someone has changed the tuple after my read
					if (content_ref.GetTimestamp() != access_ptr->timestamp_) {
						UPDATE_CC_ABORT_COUNT(thread_id_, context->txn_type_, access_ptr->table_id_);
						is_success = false;
						break;
					}
				}
				else {
					// insert_only or delete_only
					content_ref.AcquireWriteLock();
				}
			}

			// step 2: if success, then overwrite and commit
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
				uint64_t commit_ts = GenerateScalableTimestamp(curr_epoch, max_rw_ts);
#else
				uint64_t commit_ts = GenerateMonotoneTimestamp(curr_epoch, GlobalTimestamp::GetMonotoneTimestamp());
#endif
				END_CC_TS_ALLOC_TIME_MEASURE(thread_id_);

				for (size_t i = 0; i < access_list_.access_count_; ++i) {
					Access *access_ptr = access_list_.GetAccess(i);
					SchemaRecord *global_record_ptr = access_ptr->access_record_->record_;
					SchemaRecord *local_record_ptr = access_ptr->local_record_;
					auto &content_ref = access_ptr->access_record_->content_;
					if (access_ptr->access_type_ == READ_WRITE) {
						assert(commit_ts > access_ptr->timestamp_);
						global_record_ptr->CopyFrom(local_record_ptr);
						COMPILER_MEMORY_FENCE;
						content_ref.SetTimestamp(commit_ts);
					}
					else if (access_ptr->access_type_ == INSERT_ONLY) {
						assert(commit_ts > access_ptr->timestamp_);
						global_record_ptr->is_visible_ = true;
						COMPILER_MEMORY_FENCE;
						content_ref.SetTimestamp(commit_ts);
					}
					else if (access_ptr->access_type_ == DELETE_ONLY) {
						assert(commit_ts > access_ptr->timestamp_);
						global_record_ptr->is_visible_ = false;
						COMPILER_MEMORY_FENCE;
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

				// step 3: release locks and clean up.
				for (size_t i = 0; i < access_list_.access_count_; ++i) {
					Access *access_ptr = access_list_.GetAccess(i);
					if (access_ptr->access_type_ == READ_ONLY) {
						access_ptr->access_record_->content_.ReleaseReadLock();
					}
					else if (access_ptr->access_type_ == READ_WRITE) {
						access_ptr->access_record_->content_.ReleaseWriteLock();
						BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
						SchemaRecord *local_record_ptr = access_ptr->local_record_;
						MemAllocator::Free(local_record_ptr->data_ptr_);
						local_record_ptr->~SchemaRecord();
						MemAllocator::Free((char*)local_record_ptr);
						END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
					}
					else{
						// insert_only or delete_only
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
						SchemaRecord *local_record_ptr = access_ptr->local_record_;
						access_ptr->access_record_->content_.ReleaseWriteLock();
						BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
						MemAllocator::Free(local_record_ptr->data_ptr_);
						local_record_ptr->~SchemaRecord();
						MemAllocator::Free((char*)local_record_ptr);
						END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
					}
					else{
						// insert_only or delete_only
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

		void TransactionManager::AbortTransaction() {
			assert(false);
		}
	}
}

#endif
