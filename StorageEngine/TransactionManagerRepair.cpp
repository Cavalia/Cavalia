#if defined(REPAIR)
#include "TransactionManager.h"

namespace Cavalia{
	namespace StorageEngine{
		bool TransactionManager::InsertRecord(TxnContext *context, const size_t &table_id, const std::string &primary_key, SchemaRecord *record){
			BEGIN_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			Insertion *insertion = insertion_lists_[table_id].NewInsertion();
			insertion->local_record_ = record;
			insertion->table_id_ = table_id;
			END_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			return true;
		}

		bool TransactionManager::SelectRecordCC(TxnContext *context, const size_t &table_id, TableRecord *t_record, SchemaRecord *&s_record, const AccessType access_type, const size_t &access_id, bool is_key_access) {
			if (access_type == READ_ONLY) {
				Access *access = access_lists_[table_id].NewAccess();
				access->access_type_ = READ_ONLY;
				access->access_record_ = t_record;
				access->timestamp_ = t_record->content_.GetTimestamp();
				s_record = t_record->record_;

				access->table_id_ = table_id;
				access->access_id_ = access_id;
				// preserve information for repair.
				if (context->is_adhoc_ == false && context->is_dependent_ == true){
					if (is_key_access == true){
						access_caches_[table_id].Add(access_id, access);
					}
					else{
						accesses_caches_[table_id].Add(access_id, access);
					}
				}
				return true;
			}
			else if (access_type == READ_WRITE) {
				Access *access = access_lists_[table_id].NewAccess();
				access->access_type_ = READ_WRITE;
				access->access_record_ = t_record;
				// copy data
				BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
				char *local_data = allocator_->Alloc(t_record->record_->schema_ptr_->GetSchemaSize());
				SchemaRecord *local_record = (SchemaRecord*)allocator_->Alloc(sizeof(SchemaRecord));
				new(local_record)SchemaRecord(t_record->record_->schema_ptr_, local_data);
				END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
				access->timestamp_ = t_record->content_.GetTimestamp();
				COMPILER_MEMORY_FENCE;
				local_record->CopyFrom(t_record->record_);
				access->local_record_ = local_record;
				// reset returned record.
				s_record = local_record;

				access->table_id_ = table_id;
				access->access_id_ = access_id;
				// preserve information for repair.
				if (context->is_adhoc_ == false && context->is_dependent_ == true){
					if (is_key_access == true){
						access_caches_[table_id].Add(access_id, access);
					}
					else{
						accesses_caches_[table_id].Add(access_id, access);
					}
				}
				return true;
			}
			else {
				assert(access_type == DELETE_ONLY);
				Access *access = access_lists_[table_id].NewAccess();
				access->access_type_ = DELETE_ONLY;
				access->access_record_ = t_record;
				s_record = t_record->record_;

				// delete-only operations do not need to be validated.
				if (context->is_adhoc_ == false && context->is_dependent_ == true){
					if (is_key_access == true){
						access_caches_[table_id].Add(access_id, access);
					}
					else{
						accesses_caches_[table_id].Add(access_id, access);
					}
				}
				return true;
			}
		}

		bool TransactionManager::CommitTransaction(TxnContext *context, EventTuple *param, CharArray &ret_str){
			if (context->is_adhoc_ == true){
				return CommitAdhocTransaction(context, param, ret_str);
			}
			BEGIN_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			// step 1: acquire lock and validate
			uint64_t max_write_ts = 0;
			bool is_success = true;
			size_t lock_count = 0;
			while (true){
				Access *abort_access = NULL;
				for (size_t tid = 0; tid < table_count_; ++tid){
					access_lists_[tid].Sort();
					for (size_t i = 0; i < access_lists_[tid].access_count_; ++i){
						Access *access_ptr = access_lists_[tid].GetAccess(i);
						if (access_ptr->is_locked_ == true){
							continue;
						}
						// if reach here, then must be locked later.
						access_ptr->is_locked_ = true;
						++lock_count;
						if (access_ptr->access_type_ == READ_ONLY){
							// acquire read lock
							access_ptr->access_record_->content_.AcquireReadLock();
							// whether someone has changed the tuple after my read
							if (access_ptr->access_record_->content_.GetTimestamp() != access_ptr->timestamp_){
								UPDATE_CC_ABORT_COUNT(thread_id_, context->txn_type_, access_ptr->table_id_);
								// check whether repairable.
								if (ReExecute(context, param, access_ptr, ret_str) == false){
									is_success = false;
									abort_access = access_ptr;
									break;
								}
								access_ptr->timestamp_ = access_ptr->access_record_->content_.GetTimestamp();
							}
						}
						else if (access_ptr->access_type_ == READ_WRITE){
							// acquire write lock
							access_ptr->access_record_->content_.AcquireWriteLock();
							// whether someone has changed the tuple after my read
							if (access_ptr->access_record_->content_.GetTimestamp() != access_ptr->timestamp_ || access_ptr->is_affected_ == true){
								UPDATE_CC_ABORT_COUNT(thread_id_, context->txn_type_, access_ptr->table_id_);
								// check whether repairable.
								if (ReExecute(context, param, access_ptr, ret_str) == false){
									is_success = false;
									abort_access = access_ptr;
									break;
								}
								access_ptr->timestamp_ = access_ptr->access_record_->content_.GetTimestamp();
							}
							if (access_ptr->timestamp_ > max_write_ts){
								max_write_ts = access_ptr->timestamp_;
							}
						}
						else {
							assert(access_ptr->access_type_ == DELETE_ONLY);
							access_ptr->access_record_->content_.AcquireWriteLock();
						}
					}
					if (is_success == false){
						break;
					}
				}
				// if already succeeded, then break directly.
				if (is_success == true){
					break;
				}
				// otherwise, try reconstruct.
				else if (ReConstruct(context, param, abort_access) == false){
					break;
				}
				// if reconstruction fails, then abort.
				else{
					is_success = true;
				}
			}

			// step 2: then overwrite and commit
			if (is_success == true){
				BEGIN_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
				uint64_t curr_ts = ScalableTimestamp::GetTimestamp();
				END_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
				uint64_t commit_ts = GenerateTimestamp(curr_ts, max_write_ts);

				for (size_t tid = 0; tid < table_count_; ++tid){
					for (size_t i = 0; i < access_lists_[tid].access_count_; ++i){
						Access *access_ptr = access_lists_[tid].GetAccess(i);
						TableRecord *access_record = access_ptr->access_record_;
						if (access_ptr->access_type_ == READ_WRITE){
							assert(commit_ts > access_ptr->timestamp_);
							access_record->record_->CopyFrom(access_ptr->local_record_);
							COMPILER_MEMORY_FENCE;
							access_record->content_.SetTimestamp(commit_ts);
#if defined(VALUE_LOGGING)
							log_buffer_.UpdateRecord(access_record->schema_ptr_->GetTableId(), access_ptr->local_record_->data_ptr_, access_record->schema_ptr_->GetSchemaSize());
#endif
						}
						else if (access_ptr->access_type_ == DELETE_ONLY){
							assert(max_write_ts >= access_ptr->timestamp_);
							assert(commit_ts > access_ptr->timestamp_);
							access_record->record_->is_visible_ = false;
							COMPILER_MEMORY_FENCE;
							access_record->content_.SetTimestamp(commit_ts);
#if defined(VALUE_LOGGING)
							log_buffer_.DeleteRecord(access_record->schema_ptr_->GetTableId(), access_record->GetPrimaryKey());
#endif
						}
					}
				}
				for (size_t tid = 0; tid < table_count_; ++tid){
					for (size_t i = 0; i < insertion_lists_[tid].insertion_count_; ++i) {
						Insertion *insertion_ptr = insertion_lists_[tid].GetInsertion(i);
						TableRecord *tb_record = new TableRecord(insertion_ptr->local_record_);
						tb_record->content_.AcquireWriteLock();
						tb_record->content_.SetTimestamp(commit_ts);
						insertion_ptr->insertion_record_ = tb_record;
						//storage_manager_->tables_[insertion_ptr->table_id_]->InsertRecord(insertion_ptr->insertion_record_);
#if defined(VALUE_LOGGING)
						log_buffer_.InsertRecord(insertion_ptr->table_id_, insertion_record->data_ptr_, insertion_record->schema_ptr_->GetSchemaSize());
#endif
					}
				}
				// commit.
#if defined(VALUE_LOGGING)
				CharArray *log_str = log_buffer_.Commit();
				logger_->CommitTransaction(this->thread_id_, commit_ts, log_str);
#elif defined(COMMAND_LOGGING)
				CharArray *log_str = log_buffer_.Commit(context->txn_type_, param);
				logger_->CommitTransaction(this->thread_id_, commit_ts, log_str);
#endif

				// step 3: release locks and clean up.
				for (size_t tid = 0; tid < table_count_; ++tid) {
					for (size_t i = 0; i < access_lists_[tid].access_count_; ++i){
						Access *access_ptr = access_lists_[tid].GetAccess(i);
						if (access_ptr->access_type_ == READ_ONLY) {
							access_ptr->access_record_->content_.ReleaseReadLock();
						}
						else if (access_ptr->access_type_ == READ_WRITE) {
							access_ptr->access_record_->content_.ReleaseWriteLock();
							BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
							allocator_->Free(access_ptr->local_record_->data_ptr_);
							access_ptr->local_record_->~SchemaRecord();
							allocator_->Free((char*)access_ptr->local_record_);
							END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
						}
						else if (access_ptr->access_type_ == DELETE_ONLY) {
							access_ptr->access_record_->content_.ReleaseWriteLock();
						}
					}
				}
				for (size_t tid = 0; tid < table_count_; ++tid){
					for (size_t i = 0; i < insertion_lists_[tid].insertion_count_; ++i) {
						Insertion *insertion_ptr = insertion_lists_[tid].GetInsertion(i);
						insertion_ptr->insertion_record_->content_.ReleaseWriteLock();
					}
				}
			}
			// if false.
			else {
				// step 3: release locks and clean up.
				for (size_t tid = 0; tid < table_count_; ++tid) {
					for (size_t i = 0; i < access_lists_[tid].access_count_; ++i){
						Access *access_ptr = access_lists_[tid].GetAccess(i);
						if (access_ptr->access_type_ == READ_ONLY) {
							access_ptr->access_record_->content_.ReleaseReadLock();
						}
						else if (access_ptr->access_type_ == READ_WRITE) {
							access_ptr->access_record_->content_.ReleaseWriteLock();
							BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
							allocator_->Free(access_ptr->local_record_->data_ptr_);
							access_ptr->local_record_->~SchemaRecord();
							allocator_->Free((char*)access_ptr->local_record_);
							END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
						}
						else {
							assert(access_ptr->access_type_ == DELETE_ONLY);
							access_ptr->access_record_->content_.ReleaseWriteLock();
						}
						--lock_count;
						if (lock_count == 0) {
							break;
						}
					}
					if (lock_count == 0) {
						break;
					}
				}
				for (size_t tid = 0; tid < table_count_; ++tid){
					for (size_t i = 0; i < insertion_lists_[tid].insertion_count_; ++i) {
						Insertion *insertion_ptr = insertion_lists_[tid].GetInsertion(i);
						BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
						allocator_->Free(insertion_ptr->local_record_->data_ptr_);
						insertion_ptr->local_record_->~SchemaRecord();
						allocator_->Free((char*)insertion_ptr->local_record_);
						END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
					}
				}
			}

			for (size_t tid = 0; tid < table_count_; ++tid) {
				if (context->is_dependent_ == true){
					access_caches_[tid].Clear();
					accesses_caches_[tid].Clear();
				}
				access_lists_[tid].Clear();
				insertion_lists_[tid].Clear();
			}
			END_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			return is_success;
		}		
		
		bool TransactionManager::CommitAdhocTransaction(TxnContext *context, EventTuple *param, CharArray &ret_str){
			BEGIN_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			// step 1: acquire lock and validate
			uint64_t max_write_ts = 0;
			size_t lock_count = 0;
			bool is_success = true;
			for (size_t tid = 0; tid < table_count_; ++tid){
				access_lists_[tid].Sort();
				for (size_t i = 0; i < access_lists_[tid].access_count_; ++i){
					Access *access_ptr = access_lists_[tid].GetAccess(i);
					++lock_count;
					if (access_ptr->access_type_ == READ_ONLY) {
						// acquire read lock
						access_ptr->access_record_->content_.AcquireReadLock();
						// whether someone has changed the tuple after my read
						if (access_ptr->access_record_->content_.GetTimestamp() != access_ptr->timestamp_) {
							UPDATE_CC_ABORT_COUNT(thread_id_, context->txn_type_, access_ptr->access_record_->GetTableId());
							is_success = false;
							break;
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
						if (access_ptr->timestamp_ > max_write_ts) {
							max_write_ts = access_ptr->timestamp_;
						}
					}
					else {
						assert(access_ptr->access_type_ == DELETE_ONLY);
						access_ptr->access_record_->content_.AcquireWriteLock();
					}
				}
				if (is_success == false) {
					break;
				}
			}
			// step 2: if success, then overwrite and commit
			if (is_success == true) {
				BEGIN_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
				uint64_t curr_ts = ScalableTimestamp::GetTimestamp();
				END_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
				uint64_t commit_ts = GenerateTimestamp(curr_ts, max_write_ts);

				for (size_t tid = 0; tid < table_count_; ++tid){
					for (size_t i = 0; i < access_lists_[tid].access_count_; ++i) {
						Access *access_ptr = access_lists_[tid].GetAccess(i);
						TableRecord *access_record = access_ptr->access_record_;
						if (access_ptr->access_type_ == READ_WRITE) {
							assert(commit_ts > access_ptr->timestamp_);
							access_record->record_->CopyFrom(access_ptr->local_record_);
							COMPILER_MEMORY_FENCE;
							access_record->content_.SetTimestamp(commit_ts);
#if defined(VALUE_LOGGING)
							log_buffer_.UpdateRecord(access_record->schema_ptr_->GetTableId(), access_ptr->local_record_->data_ptr_, access_record->schema_ptr_->GetSchemaSize());
#endif
						}
						else if (access_ptr->access_type_ == DELETE_ONLY) {
							access_record->record_->is_visible_ = false;
							COMPILER_MEMORY_FENCE;
							access_record->content_.SetTimestamp(commit_ts);
#if defined(VALUE_LOGGING)
							log_buffer_.DeleteRecord(access_record->schema_ptr_->GetTableId(), access_record->GetPrimaryKey());
#endif
						}
					}
				}
				for (size_t tid = 0; tid < table_count_; ++tid){
					for (size_t i = 0; i < insertion_lists_[tid].insertion_count_; ++i) {
						Insertion *insertion_ptr = insertion_lists_[tid].GetInsertion(i);
						TableRecord *tb_record = new TableRecord(insertion_ptr->local_record_);
						tb_record->content_.AcquireWriteLock();
						tb_record->content_.SetTimestamp(commit_ts);
						insertion_ptr->insertion_record_ = tb_record;
						//storage_manager_->tables_[insertion_ptr->table_id_]->InsertRecord(insertion_ptr->primary_key_, insertion_ptr->insertion_record_);
#if defined(VALUE_LOGGING)
						log_buffer_.InsertRecord(insertion_ptr->table_id_, insertion_record->data_ptr_, insertion_record->schema_ptr_->GetSchemaSize());
#endif
					}
				}
				// commit.
#if defined(VALUE_LOGGING)
				CharArray *log_str = log_buffer_.Commit();
				logger_->CommitTransaction(this->thread_id_, commit_ts, log_str);
#elif defined(COMMAND_LOGGING)
				CharArray *log_str = log_buffer_.Commit(context->txn_type_, param);
				logger_->CommitTransaction(this->thread_id_, commit_ts, log_str);
#endif

				// step 3: release locks and clean up.
				for (size_t tid = 0; tid < table_count_; ++tid){
					for (size_t i = 0; i < access_lists_[tid].access_count_; ++i) {
						Access *access_ptr = access_lists_[tid].GetAccess(i);
						if (access_ptr->access_type_ == READ_ONLY) {
							access_ptr->access_record_->content_.ReleaseReadLock();
						}
						else if (access_ptr->access_type_ == READ_WRITE) {
							access_ptr->access_record_->content_.ReleaseWriteLock();
							BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
							allocator_->Free(access_ptr->local_record_->data_ptr_);
							access_ptr->local_record_->~SchemaRecord();
							allocator_->Free((char*)access_ptr->local_record_);
							END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
						}
						else if (access_ptr->access_type_ == DELETE_ONLY) {
							access_ptr->access_record_->content_.ReleaseWriteLock();
						}
					}
				}
				for (size_t tid = 0; tid < table_count_; ++tid){
					for (size_t i = 0; i < insertion_lists_[tid].insertion_count_; ++i) {
						Insertion *insertion_ptr = insertion_lists_[tid].GetInsertion(i);
						insertion_ptr->insertion_record_->content_.ReleaseWriteLock();
					}
				}
			}
			// if failed.
			else {
				// step 3: release locks and clean up.
				for (size_t tid = 0; tid < table_count_; ++tid){
					for (size_t i = 0; i < access_lists_[tid].access_count_; ++i) {
						Access *access_ptr = access_lists_[tid].GetAccess(i);
						if (access_ptr->access_type_ == READ_ONLY) {
							access_ptr->access_record_->content_.ReleaseReadLock();
						}
						else if (access_ptr->access_type_ == READ_WRITE) {
							access_ptr->access_record_->content_.ReleaseWriteLock();
							BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
							allocator_->Free(access_ptr->local_record_->data_ptr_);
							access_ptr->local_record_->~SchemaRecord();
							allocator_->Free((char*)access_ptr->local_record_);
							END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
						}
						else {
							assert(access_ptr->access_type_ == DELETE_ONLY);
							access_ptr->access_record_->content_.ReleaseWriteLock();
						}
						--lock_count;
						if (lock_count == 0) {
							break;
						}
					}
					if (lock_count == 0) {
						break;
					}
				}
				assert(lock_count == 0);
				for (size_t tid = 0; tid < table_count_; ++tid){
					for (size_t i = 0; i < insertion_lists_[tid].insertion_count_; ++i) {
						Insertion *insertion_ptr = insertion_lists_[tid].GetInsertion(i);
						BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
						allocator_->Free(insertion_ptr->local_record_->data_ptr_);
						insertion_ptr->local_record_->~SchemaRecord();
						allocator_->Free((char*)insertion_ptr->local_record_);
						END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
					}
				}
			}
			for (size_t tid = 0; tid < table_count_; ++tid) {
				access_lists_[tid].Clear();
				insertion_lists_[tid].Clear();
			}
			END_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			return is_success;
		}

		void TransactionManager::AbortTransaction() {
			assert(false);
		}
	}
}

#endif
