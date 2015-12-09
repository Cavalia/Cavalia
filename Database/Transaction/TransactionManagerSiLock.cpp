#if defined(SILOCK)
#include "TransactionManager.h"

namespace Cavalia{
	namespace Database{
		bool TransactionManager::InsertRecord(TxnContext *context, const size_t &table_id, const std::string &primary_key, SchemaRecord *record){
			BEGIN_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			if (is_first_access_ == true){
				start_timestamp_ = GlobalTimestamp::GetMaxTimestamp();
				is_first_access_ = false;
			}
			record->is_visible_ = false;
			TableRecord *tb_record = new TableRecord(record);
			//if (storage_manager_->tables_[table_id]->InsertRecord(primary_key, tb_record) == true){
			if (tb_record->content_.TryWriteLock() == false){
				this->AbortTransaction();
				return false;
			}
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
			if (is_first_access_ == true) {
				start_timestamp_ = GlobalTimestamp::GetMaxTimestamp();
				is_first_access_ = false;
			}
			// acquire write lock.
			auto &content_ref = t_record->content_;
			if (access_type == READ_WRITE){
				if (content_ref.TryWriteLock() == false){
					this->AbortTransaction();
					return false;
				}
				else if (content_ref.Validate(start_timestamp_) == false){
					content_ref.ReleaseWriteLock();
					this->AbortTransaction();
					return false;
				}
			}
			if (access_type == READ_ONLY) {
				Access *access = access_list_.NewAccess();
				access->access_type_ = access_type;
				access->access_record_ = t_record;
				access->table_id_ = table_id;
				SchemaRecord *local_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
				char* tmp_data = NULL;
				content_ref.ReadAccess(start_timestamp_, tmp_data); // read never abort or block
				new(local_record)SchemaRecord(t_record->record_->schema_ptr_, tmp_data);
				access->local_record_ = local_record;
				s_record = local_record;
				return true;
			}
			else if (access_type == READ_WRITE){
				Access *access = access_list_.NewAccess();
				access->access_type_ = access_type;
				access->access_record_ = t_record;
				access->table_id_ = table_id;
				SchemaRecord *local_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
				// write in local copy
				char* tmp_data = NULL;
				t_record->content_.ReadAccess(start_timestamp_, tmp_data); // read never abort or block
				const RecordSchema *schema_ptr = t_record->record_->schema_ptr_;
				size_t size = schema_ptr->GetSchemaSize();
				char* local_data = MemAllocator::Alloc(size);
				memcpy(local_data, tmp_data, size);
				new(local_record)SchemaRecord(schema_ptr, local_data);
				access->local_record_ = local_record;
				s_record = local_record;
				return true;
			}
			else{
				assert(false);
				return true;
			}
		}

		bool TransactionManager::CommitTransaction(TxnContext *context, TxnParam *param, CharArray &ret_str){
			BEGIN_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			uint64_t commit_timestamp = GlobalTimestamp::GetMonotoneTimestamp();
			//install writes
			for (size_t i = 0; i < access_list_.access_count_; ++i){
				Access *access_ptr = access_list_.GetAccess(i);
				auto &content_ref = access_ptr->access_record_->content_;
				if (access_ptr->access_type_ == READ_WRITE){
					// update
					content_ref.WriteAccess(commit_timestamp, access_ptr->local_record_->data_ptr_);
				}
				else if (access_ptr->access_type_ == INSERT_ONLY){
					// insert
					content_ref.SetTimestamp(commit_timestamp);
					access_ptr->access_record_->record_->is_visible_ = true;
				}
			}

			//release lock
			for (size_t i = 0; i < access_list_.access_count_; ++i){
				Access *access_ptr = access_list_.GetAccess(i);
				if (access_ptr->access_type_ == READ_WRITE){
					access_ptr->access_record_->content_.ReleaseWriteLock();
				}
				else if (access_ptr->access_type_ == INSERT_ONLY){
					access_ptr->access_record_->content_.ReleaseWriteLock();
				}
			}
			// cleanup
			for (size_t i = 0; i < access_list_.access_count_; ++i){
				Access *access_ptr = access_list_.GetAccess(i);
				// data_ptr in local records of both read and write should be alive
				if (access_ptr->access_type_ == READ_ONLY || access_ptr->access_type_ == READ_WRITE){
					SchemaRecord *local_record_ptr = access_ptr->local_record_;
					local_record_ptr->data_ptr_ = NULL;
					local_record_ptr->~SchemaRecord();
					MemAllocator::Free((char*)local_record_ptr);
				}
			}
			GlobalTimestamp::SetThreadTimestamp(thread_id_, commit_timestamp);
			assert(access_list_.access_count_ <= kMaxAccessNum);
			access_list_.Clear();
			is_first_access_ = true;
			END_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			return true;
		}

		void TransactionManager::AbortTransaction() {
			for (size_t i = 0; i < access_list_.access_count_; ++i){
				Access *access_ptr = access_list_.GetAccess(i);
				if (access_ptr->access_type_ == READ_ONLY){
					SchemaRecord *local_record_ptr = access_ptr->local_record_;
					local_record_ptr->data_ptr_ = NULL;
					local_record_ptr->~SchemaRecord();
					MemAllocator::Free((char*)local_record_ptr);
				}
				else if (access_ptr->access_type_ == READ_WRITE){
					access_ptr->access_record_->content_.ReleaseWriteLock();
					SchemaRecord *local_record_ptr = access_ptr->local_record_;
					MemAllocator::Free(local_record_ptr->data_ptr_);
					local_record_ptr->~SchemaRecord();
					MemAllocator::Free((char*)local_record_ptr);
				}
				else if (access_ptr->access_type_ == INSERT_ONLY){
					access_ptr->access_record_->content_.ReleaseWriteLock();
				}
			}
			assert(access_list_.access_count_ <= kMaxAccessNum);
			access_list_.Clear();
			is_first_access_ = true;
		}
	}
}

#endif