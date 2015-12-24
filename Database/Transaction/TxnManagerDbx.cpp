#if defined(DBX)
#include "TransactionManager.h"

namespace Cavalia{
	namespace Database{
		bool TransactionManager::InsertRecord(TxnContext *context, const size_t &table_id, const std::string &primary_key, SchemaRecord *record) {
			BEGIN_PHASE_MEASURE(thread_id_, INSERT_PHASE);
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
				// increment reference counter.
				t_record->content_.IncrementCounter();
				// ensure consistent view of timestamp_ and record_
				rtm_lock_->Lock();
				access->timestamp_ = t_record->content_.GetTimestamp();
				s_record = t_record->record_;
				rtm_lock_->Unlock();
				return true;
			}
			else if (access_type == READ_WRITE) {
				Access *access = access_list_.NewAccess();
				access->access_type_ = READ_WRITE;
				access->access_record_ = t_record;
				access->table_id_ = table_id;
				// increment reference counter.
				t_record->content_.IncrementCounter();
				// ensure consistent view of timestamp_ and record_
				SchemaRecord *global_record = NULL;
				rtm_lock_->Lock();
				access->timestamp_ = t_record->content_.GetTimestamp();
				global_record = t_record->record_;
				rtm_lock_->Unlock();
				// copy data
				BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
				const RecordSchema *schema_ptr = global_record->schema_ptr_;
				char *local_data = MemAllocator::Alloc(schema_ptr->GetSchemaSize());
				SchemaRecord *local_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
				new(local_record)SchemaRecord(schema_ptr, local_data);
				END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
				local_record->CopyFrom(global_record);
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
				access->table_id_ = table_id;
				s_record = t_record->record_;
				return true;
			}
		}

		bool TransactionManager::CommitTransaction(TxnContext *context, TxnParam *param, CharArray &ret_str){
			BEGIN_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			// step 1: acquire lock and validate
			bool is_success = true;
			
			// begin hardware transaction.
			rtm_lock_->Lock();
			for (size_t i = 0; i < access_list_.access_count_; ++i) {
				Access *access_ptr = access_list_.GetAccess(i);
				if (access_ptr->access_type_ == READ_ONLY) {
					// whether someone has changed the tuple after my read
					if (access_ptr->access_record_->content_.GetTimestamp() != access_ptr->timestamp_) {
						UPDATE_CC_ABORT_COUNT(thread_id_, context->txn_type_, access_ptr->table_id_);
						is_success = false;
						break;
					}
				}
				else if (access_ptr->access_type_ == READ_WRITE) {
					// whether someone has changed the tuple after my read
					if (access_ptr->access_record_->content_.GetTimestamp() != access_ptr->timestamp_) {
						UPDATE_CC_ABORT_COUNT(thread_id_, context->txn_type_, access_ptr->table_id_);
						is_success = false;
						break;
					}
				}
			}
			// step 2: if success, then overwrite and commit
			if (is_success == true) {
				// get global epoch id for commit
				BEGIN_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
				uint64_t curr_epoch = Epoch::GetEpoch();
				END_CC_TS_ALLOC_TIME_MEASURE(thread_id_);

				for (size_t i = 0; i < access_list_.access_count_; ++i) {
					Access *access_ptr = access_list_.GetAccess(i);
					TableRecord *access_record = access_ptr->access_record_;
					if (access_ptr->access_type_ == READ_WRITE) {
						// exchanging pointers, the old version would be recycled
						std::swap(access_record->record_, access_ptr->local_record_);
						access_ptr->timestamp_ = access_record->content_.IncrementTimestamp();
					}
					else if (access_ptr->access_type_ == INSERT_ONLY){
						access_ptr->access_record_->record_->is_visible_ = true;
					}
					else if (access_ptr->access_type_ == DELETE_ONLY) {
						access_record->record_->is_visible_ = false;
						// update timestamp to invalidate concurrent reads
						access_ptr->timestamp_ = access_record->content_.IncrementTimestamp();
					}
				}
				// end hardware transaction.
				rtm_lock_->Unlock();

				// logging, outside rtm region

				// clean up.
				for (size_t i = 0; i < access_list_.access_count_; ++i) {
					Access *access_ptr = access_list_.GetAccess(i);
					if (access_ptr->access_type_ == READ_ONLY) {
						access_ptr->access_record_->content_.DecrementCounter();
					}
					else if (access_ptr->access_type_ == READ_WRITE) {
						if (access_ptr->access_record_->content_.DecrementCounter() == 0){
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
					// deletes, wait for recycling to clean up
				}
			}
			// if failed.
			else {
				// end hardware transaction.
				rtm_lock_->Unlock();
				// clean up 
				for (size_t i = 0; i < access_list_.access_count_; ++i) {
					Access *access_ptr = access_list_.GetAccess(i);
					if (access_ptr->access_type_ == READ_ONLY) {
						access_ptr->access_record_->content_.DecrementCounter();
					}
					if (access_ptr->access_type_ == READ_WRITE) {
						access_ptr->access_record_->content_.DecrementCounter();
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
