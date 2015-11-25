#if defined(SIOCC)
#include "TransactionManager.h"

namespace Cavalia{
	namespace Database{
		bool TransactionManager::InsertRecord(TxnContext *context, const size_t &table_id, const std::string &primary_key, SchemaRecord *record){
			BEGIN_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			if (is_first_access_ == true){
				start_timestamp_ = GlobalContent::GetMaxTimestamp();
				is_first_access_ = false;
			}
			Access *access = &(accesses_[access_offset_]);
			++access_offset_;
			access->access_type_ = INSERT_ONLY;
			access->access_record_ = NULL;
			access->local_record_ = record;
			access->table_id_ = table_id;
			access_set_[table_id][primary_key] = access;
			END_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			return true;
		}

		bool TransactionManager::SelectRecordCC(TxnContext *context, const size_t &table_id, const std::string &primary_key, SchemaRecord *&record, const AccessType access_type) {
			if (is_first_access_ == true) {
				start_timestamp_ = GlobalContent::GetMaxTimestamp();
				is_first_access_ = false;
			}
			char* tmp_data = NULL;
			// read never abort or block
			record->content_.ReadAccess(start_timestamp_, tmp_data);
			Access *access = &(accesses_[access_offset_]);
			++access_offset_;
			access->access_type_ = access_type;
			access->access_record_ = record;
			SchemaRecord *local_record = (SchemaRecord*)allocator_->Alloc(sizeof(SchemaRecord));
			if (access_type == READ_ONLY) {
				// directly return the versioned copy.
				new(local_record)SchemaRecord(record->schema_ptr_, tmp_data);
			}
			else {
				// write in local copy
				size_t size = record->schema_ptr_->GetSchemaSize();
				char* local_data = allocator_->Alloc(size);
				memcpy(local_data, tmp_data, size);
				new(local_record)SchemaRecord(record->schema_ptr_, local_data);
			}
			access->local_record_ = local_record;
			access_set_[table_id][primary_key] = access;
			record = local_record;
			return true;
		}

		bool TransactionManager::CommitTransaction(TxnContext *context, TxnParam *param){
			BEGIN_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			bool is_success = true;
			size_t lock_count = 0;
			// acquire locks for all writes and validate
			for (size_t tid = 0; tid < table_count_; ++tid){
				for (auto &entry : access_set_[tid]){
					if (entry.second->access_type_ == READ_WRITE){
						++lock_count;
						entry.second->access_record_->content_.AcquireWriteLock();
						if (entry.second->access_record_->content_.Validate(start_timestamp_) == false){
							is_success = false;
							break;
						}
					}
				}
				if (is_success == false){
					break;
				}
			}
			int64_t commit_timestamp = 0;
			if (is_success == true){
				// generate commit timestamp after acquiring all locks for 2 reasons
				// 1. if validation fails, we don't need to waste the effort to generate a timestamp that would not be used
				// 2. if generating commit timestamp before acquiring all locks, reads of other concurrent txn with a bigger ts will not read this new update, though SI doesn't have to respect the timestamp order
				commit_timestamp = GlobalContent::GetMonotoneTimestamp();
				//install writes
				for (size_t tid = 0; tid < table_count_; ++tid){
					for (auto &entry : access_set_[tid]){
						if (entry.second->access_type_ == READ_WRITE){
							// update
							entry.second->access_record_->content_.WriteAccess(commit_timestamp, entry.second->local_record_->data_ptr_);
						}
						else if (entry.second->access_type_ == INSERT_ONLY){
							// insert
							entry.second->local_record_->content_.SetTimestamp(commit_timestamp);
							storage_manager_->tables_[entry.second->table_id_]->InsertRecord(entry.first, entry.second->local_record_);
						}
					}
				}
			}
			//release lock
			for (size_t tid = 0; tid < table_count_; ++tid){
				for (auto &entry : access_set_[tid]){
					if (entry.second->access_type_ == READ_WRITE){
						entry.second->access_record_->content_.ReleaseWriteLock();
						--lock_count;
						if (lock_count == 0) {
							break;
						}
					}
				}
				if (lock_count == 0){
					break;
				}
			}
			assert(lock_count == 0);
			// cleanup
			if (is_success == true){
				for (size_t tid = 0; tid < table_count_; ++tid){
					for (auto &entry : access_set_[tid]){
						// data_ptr in local records of both read and write should be alive
						if (entry.second->access_type_ != INSERT_ONLY){
							entry.second->local_record_->data_ptr_ = NULL;
							entry.second->local_record_->~SchemaRecord();
							allocator_->Free((char*)entry.second->local_record_);
						}
					}
					access_set_[tid].clear();
				}
				assert(access_offset_ <= kMaxAccessNum);
				access_offset_ = 0;
				GlobalContent::SetThreadTimestamp(thread_id_, commit_timestamp);
			}
			else{
				for (size_t tid = 0; tid < table_count_; ++tid){
					for (auto &entry : access_set_[tid]){
						if (entry.second->access_type_ == READ_ONLY){
							entry.second->local_record_->data_ptr_ = NULL;
						}
						else{
							allocator_->Free(entry.second->local_record_->data_ptr_);
						}
						entry.second->local_record_->~SchemaRecord();
						allocator_->Free((char*)entry.second->local_record_);
					}
					access_set_[tid].clear();
				}
				assert(access_offset_ <= kMaxAccessNum);
				access_offset_ = 0;
			}

			is_first_access_ = true;
			END_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			return is_success;
		}

		void TransactionManager::AbortTransaction() {
			assert(false);
		}
	}
}

#endif