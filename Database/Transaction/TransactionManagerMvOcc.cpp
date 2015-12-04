#if defined(MVOCC)
#include "TransactionManager.h"

namespace Cavalia{
	namespace Database{
		bool TransactionManager::InsertRecord(TxnContext *context, const size_t &table_id, const std::string &primary_key, SchemaRecord *record){
			BEGIN_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			record->is_visible_ = false;
			TableRecord *tb_record = new TableRecord(record);
			// upsert.
			storage_manager_->tables_[table_id]->InsertRecord(primary_key, tb_record);
			Access *access = access_list_.NewAccess();
			access->access_type_ = INSERT_ONLY;
			access->access_record_ = tb_record;
			access->local_record_ = NULL;
			access->table_id_ = table_id;
			access->timestamp_ = 0;
			END_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			return true;
		}

		bool TransactionManager::SelectRecordCC(TxnContext *context, const size_t &table_id, TableRecord *t_record, SchemaRecord *&s_record, const AccessType access_type, const size_t &access_id, bool is_key_access) {
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

			char* tmp_data = NULL;
			// read never abort or block
			t_record->content_.ReadAccess(tmp_data);
			Access *access = access_list_.NewAccess();
			access->access_type_ = access_type;
			access->access_record_ = t_record;
			SchemaRecord *local_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
			if (access_type == READ_ONLY) {
				// directly return the versioned copy.
				new(local_record)SchemaRecord(t_record->record_->schema_ptr_, tmp_data);
				access->timestamp_ = t_record->content_.GetTimestamp();
			}
			else if (access_type == READ_WRITE) {
				// write in local copy
				size_t size = t_record->record_->schema_ptr_->GetSchemaSize();
				char* local_data = MemAllocator::Alloc(size);
				memcpy(local_data, tmp_data, size);
				new(local_record)SchemaRecord(t_record->record_->schema_ptr_, local_data);
				access->timestamp_ = t_record->content_.GetTimestamp();
			}
			else {
				assert(access_type == DELETE_ONLY);
				// directly return the versioned copy.
				new(local_record)SchemaRecord(t_record->record_->schema_ptr_, tmp_data);
			}
			access->local_record_ = local_record;
			s_record = local_record;
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
			uint64_t max_global_ts = 0;
			uint32_t max_local_ts = 0;
			size_t lock_count = 0;
			bool is_success = true;
			access_list_.Sort();
			// step 1: acquire lock and validate
			for (size_t i = 0; i < access_list_.access_count_; ++i) {
				++lock_count;
				Access *access_ptr = access_list_.GetAccess(i);
				if (access_ptr->access_type_ == READ_ONLY){
					access_ptr->access_record_->content_.AcquireReadLock();
					if (access_ptr->access_record_->content_.GetTimestamp() != access_ptr->timestamp_){
						UPDATE_CC_ABORT_COUNT(thread_id_, context->txn_type_, access_ptr->access_record_->GetTableId());
						is_success = false;
						break;
					}
				}
				else if (access_ptr->access_type_ == READ_WRITE){
					access_ptr->access_record_->content_.AcquireWriteLock();
					if (access_ptr->access_record_->content_.GetTimestamp() != access_ptr->timestamp_){
						UPDATE_CC_ABORT_COUNT(thread_id_, context->txn_type_, access_ptr->access_record_->GetTableId());
						is_success = false;
						break;
					}
					uint64_t access_global_ts = access_ptr->timestamp_ >> 32;
					uint32_t access_local_ts = access_ptr->timestamp_ & 0xFFFFFFFF;
					if (access_global_ts > max_global_ts) {
						max_global_ts = access_global_ts;
						max_local_ts = access_local_ts;
					}
					else if (access_global_ts == max_global_ts && access_local_ts > max_local_ts) {
						max_local_ts = access_local_ts;
					}
				}
				else {
					assert(access_ptr->access_type_ == DELETE_ONLY);
					access_ptr->access_record_->content_.AcquireWriteLock();
				}
			}
			// step 2: if success, then overwrite and commit
			uint64_t commit_ts = 0;
			if (is_success == true){
				BEGIN_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
				uint64_t curr_ts = ScalableTimestamp::GetTimestamp();
				END_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
				assert(curr_ts >= max_global_ts);
				assert(curr_ts >= this->global_ts_);
				// init.
				if (curr_ts > this->global_ts_) {
					this->global_ts_ = curr_ts;
					this->local_ts_ = this->thread_id_;
				}
				// compute commit timestamp.
				if (this->global_ts_ > max_global_ts) {
					this->local_ts_ += this->thread_count_;
				}
				else {
					assert(this->global_ts_ == max_global_ts);
					if (this->local_ts_ > max_local_ts) {
						this->local_ts_ += this->thread_count_;
					}
					else {
						this->local_ts_ = (max_local_ts / thread_count_ + 1)*thread_count_ + thread_id_;
					}
				}

				commit_ts = (this->global_ts_ << 32) | this->local_ts_;

				//install writes
				for (size_t i = 0; i < access_list_.access_count_; ++i) {
					Access *access_ptr = access_list_.GetAccess(i);
					TableRecord *access_record = access_ptr->access_record_;
					if (access_ptr->access_type_ == READ_WRITE){
						access_record->content_.WriteAccess(commit_ts, access_ptr->local_record_->data_ptr_);
					}
					else if (access_ptr->access_type_ == DELETE_ONLY){
						access_record->content_.SetTimestamp(commit_ts);
						access_record->record_->is_visible_ = false;
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
					assert(access_ptr->access_type_ == READ_WRITE || access_ptr->access_type_ == DELETE_ONLY);
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
					access_ptr->local_record_->data_ptr_ = NULL;
					access_ptr->local_record_->~SchemaRecord();
					MemAllocator::Free((char*)access_ptr->local_record_);
				}
				GlobalTimestamp::SetThreadTimestamp(thread_id_, commit_ts);
			}
			else{
				for (size_t i = 0; i < access_list_.access_count_; ++i) {
					Access *access_ptr = access_list_.GetAccess(i);
					if (access_ptr->access_type_ == READ_WRITE) {
						MemAllocator::Free(access_ptr->local_record_->data_ptr_);
					}
					else {
						assert(access_ptr->access_type_ == READ_ONLY || access_ptr->access_type_ == DELETE_ONLY);
						access_ptr->local_record_->data_ptr_ = NULL;
					}
					access_ptr->local_record_->~SchemaRecord();
					MemAllocator::Free((char*)access_ptr->local_record_);
				}
			}
			assert(access_list_.access_count_ <= kMaxAccessNum);
			access_list_.Clear();
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
