#if defined(TVLOCK)
#include "TransactionManager.h"

namespace Cavalia{
	namespace Database{
		bool TransactionManager::InsertRecord(TxnContext *context, const size_t &table_id, const std::string &primary_key, SchemaRecord *record){
			BEGIN_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			Insertion *insertion = insertion_list_.NewInsertion();
			insertion->local_record_ = record;
			insertion->table_id_ = table_id;
			insertion->primary_key_ = primary_key;
			END_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			return true;
		}

		bool TransactionManager::SelectRecordCC(TxnContext *context, const size_t &table_id, TableRecord *t_record, SchemaRecord *&s_record, const AccessType access_type, const size_t &access_id, bool is_key_access) {
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
					s_record = t_record->record_;
				}
			}
			else {
				assert(access_type == READ_WRITE);
				if (t_record->content_.AcquireWriteLock() == false) {
					this->AbortTransaction();
					return false;
				}
				else {
					// return local copy.
					char *local_data = MemAllocator::Alloc(t_record->record_->schema_ptr_->GetSchemaSize());
					SchemaRecord *local_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
					new(local_record)SchemaRecord(t_record->record_->schema_ptr_, local_data);
					t_record->record_->CopyTo(local_record);
					Access *access = access_list_.NewAccess();
					access->access_type_ = READ_WRITE;
					access->access_record_ = t_record;
					access->local_record_ = local_record;
					s_record = local_record;
				}
			}
			return true;
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
					//else if (access_ptr->access_type_ == INSERT_ONLY){
					//	// install from local copy.
					//	storage_manager_->tables_[access_ptr->table_id_]->InsertRecord(access_ptr->primary_key_, access_ptr->local_record_);
					//}
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
						MemAllocator::Free(access_ptr->local_record_->data_ptr_);
						access_ptr->local_record_->~SchemaRecord();
						MemAllocator::Free((char*)access_ptr->local_record_);
					}
				}
				assert(access_list_.access_count_ <= kMaxAccessNum);
				access_list_.Clear();
			}
			else{
				for (size_t i = 0; i < access_list_.access_count_; ++i){
					Access *access_ptr = access_list_.GetAccess(i);
					if (access_ptr->access_type_ != READ_ONLY){
						MemAllocator::Free(access_ptr->local_record_->data_ptr_);
						access_ptr->local_record_->~SchemaRecord();
						MemAllocator::Free((char*)access_ptr->local_record_);
					}
				}
				assert(access_list_.access_count_ <= kMaxAccessNum);
				access_list_.Clear();
			}
			// for mvlock, no need to set is_first_access.
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
				//else{
				//	assert(access_ptr->access_type_ == INSERT_ONLY);
				//	MemAllocator::Free(access_ptr->local_record_->data_ptr_);
				//	access_ptr->local_record_->~SchemaRecord();
				//	MemAllocator::Free((char*)access_ptr->local_record_);
				//}
			}
			assert(access_list_.access_count_ <= kMaxAccessNum);
			access_list_.Clear();
		}
	}
}

#endif