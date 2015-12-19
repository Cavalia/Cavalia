#if defined(MVOCC)
#include "TransactionManager.h"

namespace Cavalia{
	namespace Database{
		bool TransactionManager::InsertRecord(TxnContext *context, const size_t &table_id, const std::string &primary_key, SchemaRecord *record){
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
			//}
			//else{
			//	// if the record has already existed, then we need to lock the original record.
			//	END_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			//	return true;
			//}
		}

		bool TransactionManager::SelectRecordCC(TxnContext *context, const size_t &table_id, TableRecord *t_record, SchemaRecord *&s_record, const AccessType access_type) {
			if (context->is_read_only_ == true){
				if (is_first_access_ == true){
					BEGIN_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
					start_timestamp_ = GlobalTimestamp::GetMaxTimestamp();
					is_first_access_ = false;
					END_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
				}
				char* tmp_data = NULL;
				t_record->content_.ReadAccess(start_timestamp_, tmp_data);
				SchemaRecord *local_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
				new(local_record)SchemaRecord(t_record->record_->schema_ptr_, tmp_data);
				read_only_set_.push_back(local_record);
				s_record = local_record;
				return true;
			}
			////////////////////////////////////////////

			if (access_type == READ_ONLY) {
				Access *access = access_list_.NewAccess();
				access->access_type_ = READ_ONLY;
				access->access_record_ = t_record;
				access->table_id_ = table_id;
				SchemaRecord *local_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
				access->timestamp_ = t_record->content_.GetTimestamp();
				COMPILER_MEMORY_FENCE;
				char* tmp_data = NULL;
				t_record->content_.ReadAccess(tmp_data); // read never abort or block
				new(local_record)SchemaRecord(t_record->record_->schema_ptr_, tmp_data);
				access->local_record_ = local_record;
				s_record = local_record;
				return true;
			}
			else if (access_type == READ_WRITE) {
				Access *access = access_list_.NewAccess();
				access->access_type_ = READ_WRITE;
				access->access_record_ = t_record;
				access->table_id_ = table_id;
				SchemaRecord *local_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
				access->timestamp_ = t_record->content_.GetTimestamp();
				COMPILER_MEMORY_FENCE;
				char* tmp_data = NULL;
				t_record->content_.ReadAccess(tmp_data); // read never abort or block
				const RecordSchema *schema_ptr = t_record->record_->schema_ptr_;
				// write in local copy
				size_t size = schema_ptr->GetSchemaSize();
				char* local_data = MemAllocator::Alloc(size);
				memcpy(local_data, tmp_data, size);
				new(local_record)SchemaRecord(schema_ptr, local_data);
				access->local_record_ = local_record;
				s_record = local_record;
				return true;
			}
			else {
				assert(false);
				assert(access_type == DELETE_ONLY);
				// directly return the versioned copy.
				//new(local_record)SchemaRecord(t_record->record_->schema_ptr_, tmp_data);
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
			uint64_t max_rw_ts = 0;
			size_t lock_count = 0;
			bool is_success = true;
			access_list_.Sort();
			// step 1: acquire lock and validate
			for (size_t i = 0; i < access_list_.access_count_; ++i) {
				++lock_count;
				Access *access_ptr = access_list_.GetAccess(i);
				auto &content_ref = access_ptr->access_record_->content_;
				if (access_ptr->access_type_ == READ_ONLY){
					// acquire read lock.
					content_ref.AcquireReadLock();
					// whether someone has changed the tuple after my read
					if (content_ref.GetTimestamp() != access_ptr->timestamp_){
						UPDATE_CC_ABORT_COUNT(thread_id_, context->txn_type_, access_ptr->table_id_);
						is_success = false;
						break;
					}
				}
				else if (access_ptr->access_type_ == READ_WRITE){
					// acquire write lock.
					content_ref.AcquireWriteLock();
					if (content_ref.GetTimestamp() != access_ptr->timestamp_){
						UPDATE_CC_ABORT_COUNT(thread_id_, context->txn_type_, access_ptr->table_id_);
						is_success = false;
						break;
					}
				}
				else {
					// insert_only or delete_only
					content_ref.AcquireWriteLock();
				}
				if (access_ptr->timestamp_ > max_rw_ts){
					max_rw_ts = access_ptr->timestamp_;
				}
			}
			uint64_t commit_ts = 0;
			// step 2: if success, then overwrite and commit
			if (is_success == true){
				BEGIN_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
				uint64_t curr_epoch = Epoch::GetEpoch();
				END_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
				commit_ts = GenerateTimestamp(curr_epoch, max_rw_ts);

				//install writes
				for (size_t i = 0; i < access_list_.access_count_; ++i) {
					Access *access_ptr = access_list_.GetAccess(i);
					SchemaRecord *global_record_ptr = access_ptr->access_record_->record_;
					SchemaRecord *local_record_ptr = access_ptr->local_record_;
					auto &content_ref = access_ptr->access_record_->content_;
					if (access_ptr->access_type_ == READ_WRITE){
						assert(commit_ts > access_ptr->timestamp_);
						content_ref.WriteAccess(commit_ts, local_record_ptr->data_ptr_);
					}
					else if (access_ptr->access_type_ == INSERT_ONLY){
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
			}

			// step 3: release locks and clean up.
			for (size_t i = 0; i < access_list_.access_count_; ++i) {
				Access *access_ptr = access_list_.GetAccess(i);
				if (access_ptr->access_type_ == READ_ONLY){
					access_ptr->access_record_->content_.ReleaseReadLock();
				}
				else {
					access_ptr->access_record_->content_.ReleaseWriteLock();
				}
				--lock_count;
				if (lock_count == 0){
					break;
				}
			}
			assert(lock_count == 0);
			// clean up.
			if (is_success == true){
				for (size_t i = 0; i < access_list_.access_count_; ++i) {
					Access *access_ptr = access_list_.GetAccess(i);
					if (access_ptr->access_type_ == READ_WRITE || access_ptr->access_type_ == READ_ONLY){
						SchemaRecord *local_record_ptr = access_ptr->local_record_;
						local_record_ptr->data_ptr_ = NULL;
						local_record_ptr->~SchemaRecord();
						MemAllocator::Free((char*)local_record_ptr);
					}
				}
				GlobalTimestamp::SetThreadTimestamp(thread_id_, commit_ts);
			}
			else{
				for (size_t i = 0; i < access_list_.access_count_; ++i) {
					Access *access_ptr = access_list_.GetAccess(i);
					if (access_ptr->access_type_ == READ_ONLY){
						SchemaRecord *local_record_ptr = access_ptr->local_record_;
						local_record_ptr->data_ptr_ = NULL;
						local_record_ptr->~SchemaRecord();
						MemAllocator::Free((char*)local_record_ptr);
					}
					else if (access_ptr->access_type_ == READ_WRITE) {
						SchemaRecord *local_record_ptr = access_ptr->local_record_;
						MemAllocator::Free(local_record_ptr->data_ptr_);
						local_record_ptr->~SchemaRecord();
						MemAllocator::Free((char*)local_record_ptr);
					}
					else {
						//access_ptr->local_record_->data_ptr_ = NULL;
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
