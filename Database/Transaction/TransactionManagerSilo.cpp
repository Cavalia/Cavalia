#if defined(SILO)
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
			write_list_.Add(access);
			END_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			return true;
		}

		bool TransactionManager::SelectRecordCC(TxnContext *context, const size_t &table_id, TableRecord *t_record, SchemaRecord *&s_record, const AccessType access_type, const size_t &access_id, bool is_key_access) {
			if (access_type == READ_ONLY){
				Access *access = access_list_.NewAccess();
				access->access_type_ = READ_ONLY;
				access->access_record_ = t_record;
				access->local_record_ = NULL;
				access->timestamp_ = t_record->content_.GetTimestamp();
				s_record = t_record->record_;
				return true;
			}
			else if (access_type == READ_WRITE){
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
				write_list_.Add(access);
				// reset returned record.
				s_record = local_record;
				return true;
			}
			else{
				assert(access_type == DELETE_ONLY);
				Access *access = access_list_.NewAccess();
				access->access_type_ = DELETE_ONLY;
				access->access_record_ = t_record;
				write_list_.Add(access);
				s_record = t_record->record_;
				return true;
			}
		}

		bool TransactionManager::CommitTransaction(TxnContext *context, TxnParam *param, CharArray &ret_str){
			BEGIN_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
			// step 1: acquire write lock.
			uint64_t max_rw_ts = 0;
			write_list_.Sort();
			for (size_t i = 0; i < write_list_.access_count_; ++i){
				Access *access_ptr = write_list_.accesses_[i];
				access_ptr->access_record_->content_.AcquireWriteLock();
				if (access_ptr->timestamp_ > max_rw_ts){
					max_rw_ts = access_ptr->timestamp_;
				}
			}
			// should also update readers' timestamps.

			BEGIN_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
			uint64_t global_ts = ScalableTimestamp::GetTimestamp();
			END_CC_TS_ALLOC_TIME_MEASURE(thread_id_);

			// setp 2: validate read.
			bool is_success = true;
			for (size_t i = 0; i < access_list_.access_count_; ++i){
				Access *access_ptr = access_list_.GetAccess(i);
				if (access_ptr->access_type_ == READ_WRITE){
					if (access_ptr->access_record_->content_.GetTimestamp() != access_ptr->timestamp_){
						is_success = false;
						break;
					}
				}
				else if (access_ptr->access_type_ == READ_ONLY){
					if (access_ptr->access_record_->content_.ExistsWriteLock() || 
						access_ptr->access_record_->content_.GetTimestamp() != access_ptr->timestamp_){
						is_success = false;
						break;
					}
				}
			}

			// step 3: if success, then overwrite and commit
			if (is_success == true){
				uint64_t commit_ts = GenerateTimestamp(global_ts, max_rw_ts);

				for (size_t i = 0; i < write_list_.access_count_; ++i){
					Access *access_ptr = write_list_.accesses_[i];
					TableRecord *access_record = access_ptr->access_record_;
					if (access_ptr->access_type_ == READ_WRITE){
						access_record->record_->CopyFrom(access_ptr->local_record_);
						COMPILER_MEMORY_FENCE;
						access_record->content_.SetTimestamp(commit_ts);
#if defined(VALUE_LOGGING)
						((ValueLogger*)logger_)->UpdateRecord(this->thread_id_, access_ptr->table_id_, access_ptr->local_record_->data_ptr_, access_record->record_->schema_ptr_->GetSchemaSize());
#endif
					}
					else if (access_ptr->access_type_ == INSERT_ONLY){
						access_record->record_->is_visible_ = true;
						COMPILER_MEMORY_FENCE;
						access_record->content_.SetTimestamp(commit_ts);
#if defined(VALUE_LOGGING)
						((ValueLogger*)logger_)->InsertRecord(this->thread_id_, access_ptr->table_id_, access_record->record_->data_ptr_, access_record->record_->schema_ptr_->GetSchemaSize());
#endif
					}
					else if (access_ptr->access_type_ == DELETE_ONLY){
						access_record->record_->is_visible_ = false;
						COMPILER_MEMORY_FENCE;
						access_record->content_.SetTimestamp(commit_ts);
#if defined(VALUE_LOGGING)
						((ValueLogger*)logger_)->DeleteRecord(this->thread_id_, access_ptr->table_id_, access_record->record_->GetPrimaryKey());
#endif
					}
				}
				// commit. 
#if defined(VALUE_LOGGING)
				((ValueLogger*)logger_)->CommitTransaction(this->thread_id_, global_ts, commit_ts);
#elif defined(COMMAND_LOGGING)
				((CommandLogger*)logger_)->CommitTransaction(this->thread_id_, global_ts, context->txn_type_, param);
#endif

				// step 4: release locks and clean up.
				for (size_t i = 0; i < write_list_.access_count_; ++i){
					Access *access_ptr = write_list_.accesses_[i];
					if (access_ptr->access_type_ == READ_WRITE){
						access_ptr->access_record_->content_.ReleaseWriteLock();
						BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
						MemAllocator::Free(access_ptr->local_record_->data_ptr_);
						access_ptr->local_record_->~SchemaRecord();
						MemAllocator::Free((char*)access_ptr->local_record_);
						END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
					}
					else if (access_ptr->access_type_ == INSERT_ONLY){
						access_ptr->access_record_->content_.ReleaseWriteLock();
					}
					else if (access_ptr->access_type_ == DELETE_ONLY){
						access_ptr->access_record_->content_.ReleaseWriteLock();
					}
				}
			}
			// if failed.
			else{
				// step 4: release locks and clean up.
				for (size_t i = 0; i < write_list_.access_count_; ++i){
					Access *access_ptr = write_list_.accesses_[i];
					if (access_ptr->access_type_ == READ_WRITE){
						access_ptr->access_record_->content_.ReleaseWriteLock();
						BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
						MemAllocator::Free(access_ptr->local_record_->data_ptr_);
						access_ptr->local_record_->~SchemaRecord();
						MemAllocator::Free((char*)access_ptr->local_record_);
						END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
					}
					else if (access_ptr->access_type_ == INSERT_ONLY){
						access_ptr->access_record_->content_.ReleaseWriteLock();
					}
					else if (access_ptr->access_type_ == DELETE_ONLY){
						access_ptr->access_record_->content_.ReleaseWriteLock();
					}
				}
			}
			assert(access_list_.access_count_ <= kMaxAccessNum);
			write_list_.Clear();
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
