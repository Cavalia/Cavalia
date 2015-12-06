#if defined(TVLOCK)
#include "TransactionManager.h"

namespace Cavalia{
	namespace Database{
		bool TransactionManager::InsertRecord(TxnContext *context, const size_t &table_id, const std::string &primary_key, SchemaRecord *record){
			BEGIN_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			record->is_visible_ = false;
			TableRecord *tb_record = new TableRecord(record);
			//if (storage_manager_->tables_[table_id]->InsertRecord(primary_key, tb_record) == true){
			//if (tb_record->content_.TryWriteLock() == false){
			//	this->AbortTransaction();
			//	return false;
			//}
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
			// for lock
			if (access_type == READ_ONLY) {
				// if cannot get lock, then return immediately.
				if (t_record->content_.AcquireReadLock() == false) {
					this->AbortTransaction();
					return false;
				}
				else {
					// directly return global copy.
					Access *access = access_list_.NewAccess();
					access->access_type_ = READ_ONLY;
					access->access_record_ = t_record;
					access->local_record_ = NULL;
					access->table_id_ = table_id;
					s_record = t_record->record_;
					return true;
				}
			}
			else if (access_type == READ_WRITE){
				if (t_record->content_.AcquireWriteLock() == false) {
					this->AbortTransaction();
					return false;
				}
				else {
					// return local copy.
					const RecordSchema *schema_ptr = t_record->record_->schema_ptr_;
					char *local_data = MemAllocator::Alloc(schema_ptr->GetSchemaSize());
					SchemaRecord *local_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
					new(local_record)SchemaRecord(schema_ptr, local_data);
					t_record->record_->CopyTo(local_record);
					Access *access = access_list_.NewAccess();
					access->access_type_ = READ_WRITE;
					access->access_record_ = t_record;
					access->local_record_ = local_record;
					access->table_id_ = table_id;
					s_record = local_record;
					return true;
				}
			}
			else{
				assert(false);
				return true;
			}
		}

		bool TransactionManager::CommitTransaction(TxnContext *context, TxnParam *param, CharArray &ret_str){
			BEGIN_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			bool is_success = true;
			// count number of certify locks.
			size_t certify_count = 0;
			// upgrade write lock to certify lock.
			for (size_t i = 0; i < access_list_.access_count_; ++i){
				Access *access_ptr = access_list_.GetAccess(i);
				if (access_ptr->access_type_ == READ_WRITE){
					// try to upgrade to certify lock.
					if (access_ptr->access_record_->content_.AcquireCertifyLock() == false){
						is_success = false;
						break;
					}
					else{
						++certify_count;
					}
				}
			}
			if (is_success == true){
				// install.
				for (size_t i = 0; i < access_list_.access_count_; ++i){
					Access *access_ptr = access_list_.GetAccess(i);
					if (access_ptr->access_type_ == READ_WRITE){
						// install from local copy.
						access_ptr->access_record_->record_->CopyFrom(access_ptr->local_record_);
					}
					else if (access_ptr->access_type_ == INSERT_ONLY){
						// install from local copy.
					}
				}
			}

			// release locks.
			for (size_t i = 0; i < access_list_.access_count_; ++i){
				Access *access_ptr = access_list_.GetAccess(i);
				if (access_ptr->access_type_ == READ_ONLY) {
					access_ptr->access_record_->content_.ReleaseReadLock();
				}
				else if (access_ptr->access_type_ == READ_WRITE) {
					if (certify_count > 0){
						access_ptr->access_record_->content_.ReleaseCertifyLock();
						--certify_count;
					}
					else{
						access_ptr->access_record_->content_.ReleaseWriteLock();
					}
				}
			}

			if (is_success == true){
				// clean up.
				for (size_t i = 0; i < access_list_.access_count_; ++i){
					Access *access_ptr = access_list_.GetAccess(i);
					if (access_ptr->access_type_ == READ_WRITE){
						SchemaRecord *local_record_ptr = access_ptr->local_record_;
						MemAllocator::Free(local_record_ptr->data_ptr_);
						local_record_ptr->~SchemaRecord();
						MemAllocator::Free((char*)local_record_ptr);
					}
				}
			}
			else{
				for (size_t i = 0; i < access_list_.access_count_; ++i){
					Access *access_ptr = access_list_.GetAccess(i);
					if (access_ptr->access_type_ == READ_WRITE){
						SchemaRecord *local_record_ptr = access_ptr->local_record_;
						MemAllocator::Free(local_record_ptr->data_ptr_);
						local_record_ptr->~SchemaRecord();
						MemAllocator::Free((char*)local_record_ptr);
					}
				}
			}
			assert(access_list_.access_count_ <= kMaxAccessNum);
			access_list_.Clear();
			// for tvlock, no need to set is_first_access.
			END_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			return is_success;
		}

		void TransactionManager::AbortTransaction() {
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
			}
			assert(access_list_.access_count_ <= kMaxAccessNum);
			access_list_.Clear();
		}
	}
}

#endif