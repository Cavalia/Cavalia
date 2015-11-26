#pragma once
#ifndef __CAVALIA_DATABASE_TRANSACTION_MANAGER_H__
#define __CAVALIA_DATABASE_TRANSACTION_MANAGER_H__

#include <unordered_set>
#include "../Meta/MetaTypes.h"
#include "../Profiler/Profilers.h"
//#include "../Logger/ValueLogger.h"
#include "../Logger/CommandLogger.h"
#include "../Content/ContentCommon.h"
#include "../Storage/TableRecords.h"
#include "../Storage/SchemaRecords.h"
#include "../Storage/BaseStorageManager.h"
#include "TxnAccess.h"
#include "TxnContext.h"
#include "GlobalTimestamp.h"
#include "BatchTimestamp.h"
#include "ScalableTimestamp.h"
#if defined(DBX)
#include <RtmLock.h>
#endif

namespace Cavalia{
	namespace Database{

		class TransactionManager{
		public:
			// for executors
			TransactionManager(BaseStorageManager *const storage_manager, BaseLogger *const logger, const size_t &thread_id, const size_t &thread_count) : storage_manager_(storage_manager), logger_(logger), thread_id_(thread_id), thread_count_(thread_count){
				table_count_ = storage_manager->table_count_;
				is_first_access_ = true;
				global_ts_ = 0;
				local_ts_ = 0;
				
#if defined(HEALING)
				access_caches_ = new AccessPtrList<kMaxAccessPerTableNum>[table_count_];
				accesses_caches_ = new AccessPtrsList<kMaxOptPerTableNum, kMaxAccessPerOptNum>[table_count_];
				access_lists_ = new AccessList<kMaxAccessPerTableNum>[table_count_];
				insertion_lists_ = new InsertionList<kMaxAccessPerTableNum>[table_count_];
#endif
				t_records_ = new TableRecords(64);
			}

			// for replayer.
			TransactionManager(BaseStorageManager *const storage_manager, BaseLogger *const logger) : storage_manager_(storage_manager), logger_(logger){}

			// destruction.
			virtual ~TransactionManager(){
#if defined(HEALING)
				delete[] access_caches_;
				access_caches_ = NULL;
				delete[] accesses_caches_;
				accesses_caches_ = NULL;
				delete[] access_lists_;
				access_lists_ = NULL;
				delete[] insertion_lists_;
				insertion_lists_ = NULL;
#endif
			}

#if defined(DBX)
			void SetRtmLock(RtmLock *rtm_lock){
				rtm_lock_ = rtm_lock;
			}
#endif

			bool InsertRecord(TxnContext *context, const size_t &table_id, const std::string &primary_key, SchemaRecord *record);
			bool InsertRecord(TxnContext *context, const size_t &table_id, SchemaRecord *record){
				return InsertRecord(context, table_id, record->GetPrimaryKey(), record);
			}

			// shared
			bool SelectKeyRecord(TxnContext *context, const size_t &table_id, const std::string &primary_key, SchemaRecord *&record, const AccessType access_type, const size_t &access_id){
				BEGIN_INDEX_TIME_MEASURE(thread_id_);
				TableRecord *t_record = NULL;
				storage_manager_->tables_[table_id]->SelectKeyRecord(primary_key, t_record);
				END_INDEX_TIME_MEASURE(thread_id_);
				if (t_record != NULL){
					BEGIN_PHASE_MEASURE(thread_id_, SELECT_PHASE);
#if defined(HEALING)
					bool rt = SelectRecordCC(context, table_id, t_record, record, access_type, access_id, true);
#else
					bool rt = SelectRecordCC(context, table_id, t_record, record, access_type);
#endif
					END_PHASE_MEASURE(thread_id_, SELECT_PHASE);
					return rt;
				}
				return true;
			}

			// partition
			bool SelectKeyRecord(TxnContext *context, const size_t &table_id, const int &partition_id, const std::string &primary_key, SchemaRecord *&record, const AccessType access_type){
				BEGIN_INDEX_TIME_MEASURE(thread_id_);
				TableRecord *t_record = NULL;
				storage_manager_->tables_[table_id]->SelectKeyRecord(partition_id, primary_key, t_record);
				END_INDEX_TIME_MEASURE(thread_id_);
				if (t_record != NULL){
					BEGIN_PHASE_MEASURE(thread_id_, SELECT_PHASE);
					bool rt = SelectRecordCC(context, table_id, t_record, record, access_type);
					END_PHASE_MEASURE(thread_id_, SELECT_PHASE);
					return rt;
				}
				return true;
			}

			// shared
			bool SelectRecord(TxnContext *context, const size_t &table_id, const size_t &idx_id, const std::string &secondary_key, SchemaRecord *&record, const AccessType access_type, const size_t &access_id){
				BEGIN_INDEX_TIME_MEASURE(thread_id_);
				TableRecord *t_record = NULL;
				storage_manager_->tables_[table_id]->SelectRecord(idx_id, secondary_key, t_record);
				END_INDEX_TIME_MEASURE(thread_id_);
				if (t_record != NULL){
					BEGIN_PHASE_MEASURE(thread_id_, SELECT_PHASE);
#if defined(HEALING)
					bool rt = SelectRecordCC(context, table_id, t_record, record, access_type, access_id, true);
#else
					bool rt = SelectRecordCC(context, table_id, t_record, record, access_type);
#endif
					END_PHASE_MEASURE(thread_id_, SELECT_PHASE);
					return rt;
				}
				return true;
			}

			// partition
			bool SelectRecord(TxnContext *context, const size_t &table_id, const int &partition_id, const size_t &idx_id, const std::string &secondary_key, SchemaRecord *&record, const AccessType access_type){
				BEGIN_INDEX_TIME_MEASURE(thread_id_);
				TableRecord *t_record = NULL;
				storage_manager_->tables_[table_id]->SelectRecord(partition_id, idx_id, secondary_key, t_record);
				END_INDEX_TIME_MEASURE(thread_id_);
				if (t_record != NULL) {
					BEGIN_PHASE_MEASURE(thread_id_, SELECT_PHASE);
					bool rt = SelectRecordCC(context, table_id, t_record, record, access_type);
					END_PHASE_MEASURE(thread_id_, SELECT_PHASE);
					return rt;
				}
				return true;
			}

			// shared
			bool SelectRecords(TxnContext *context, const size_t &table_id, const size_t &idx_id, const std::string &secondary_key, SchemaRecords *records, const AccessType access_type, const size_t &access_id) {
				BEGIN_INDEX_TIME_MEASURE(thread_id_);
				storage_manager_->tables_[table_id]->SelectRecords(idx_id, secondary_key, t_records_);
				END_INDEX_TIME_MEASURE(thread_id_);
				BEGIN_PHASE_MEASURE(thread_id_, SELECT_PHASE);
#if defined(HEALING)
				bool rt = SelectRecordsCC(context, table_id, t_records_, records, access_type, access_id);
#else
				bool rt = SelectRecordsCC(context, table_id, t_records_, records, access_type);
#endif
				t_records_->Clear();
				END_PHASE_MEASURE(thread_id_, SELECT_PHASE);
				return rt;
			}

			// partition
			bool SelectRecords(TxnContext *context, const size_t &table_id, const int &partition_id, const size_t &idx_id, const std::string &secondary_key, SchemaRecords *records, const AccessType access_type) {
				BEGIN_INDEX_TIME_MEASURE(thread_id_);
				storage_manager_->tables_[table_id]->SelectRecords(partition_id, idx_id, secondary_key, t_records_);
				END_INDEX_TIME_MEASURE(thread_id_);
				BEGIN_PHASE_MEASURE(thread_id_, SELECT_PHASE);
				bool rt = SelectRecordsCC(context, table_id, t_records_, records, access_type);
				t_records_->Clear();
				END_PHASE_MEASURE(thread_id_, SELECT_PHASE);
				return rt;
			}

			bool CommitTransaction(TxnContext *context, TxnParam *param, CharArray &ret_str);
			void AbortTransaction();


			size_t GetTableSize(const size_t &table_id) const{
				return storage_manager_->tables_[table_id]->GetTableSize();
			}

		private:
			bool SelectRecordCC(TxnContext *context, const size_t &table_id, TableRecord *t_record, SchemaRecord *&s_record, const AccessType access_type, const size_t &access_id = SIZE_MAX, bool is_key_access = true);
			bool SelectRecordsCC(TxnContext *context, const size_t &table_id, TableRecords *t_records, SchemaRecords *records, const AccessType access_type, const size_t &access_id = SIZE_MAX){
				for (size_t i = 0; i < t_records->curr_size_; ++i) {
					SchemaRecord **s_record = &(records->records_[i]);
					TableRecord *t_record = t_records->records_[i];
#if defined(HEALING)
					if (SelectRecordCC(context, table_id, t_record, *s_record, access_type, access_id, false) == false) {
						return false;
					}
#else
					if (SelectRecordCC(context, table_id, t_record, *s_record, access_type) == false) {
						return false;
					}
#endif
				}
				return true;
			}

#if defined(HEALING)
			virtual bool CommitAdhocTransaction(TxnContext *context, TxnParam *param, CharArray &ret_str);
			virtual bool ReConstruct(TxnContext *context, TxnParam *param, Access *access){ return false; }
			virtual bool ReExecute(TxnContext *context, TxnParam *param, Access *accesss, CharArray &ret_str){ return false; }
#endif

			uint64_t GenerateTimestamp(const uint64_t curr_ts, const uint64_t &max_write_ts){
				uint64_t max_global_ts = max_write_ts >> 32;
				uint32_t max_local_ts = max_write_ts & 0xFFFFFFFF;
				assert(curr_ts >= max_global_ts);
				assert(curr_ts >= this->global_ts_);
				// init.
				if (curr_ts > this->global_ts_) {
					this->global_ts_ = curr_ts;
					this->local_ts_ = this->thread_id_;
				}
				assert(this->global_ts_ == curr_ts);
				// compute commit timestamp.
				if (curr_ts == max_global_ts) {
					if (this->local_ts_ <= max_local_ts) {
						this->local_ts_ = (max_local_ts / thread_count_ + 1)*thread_count_ + thread_id_;
						assert(this->local_ts_ > max_local_ts);
					}
					assert(this->local_ts_ > max_local_ts);
				}
				assert(this->global_ts_ == max_global_ts && this->local_ts_ >= max_local_ts || this->global_ts_ > max_global_ts);

				uint64_t commit_ts = (this->global_ts_ << 32) | this->local_ts_;
				assert(commit_ts >= max_write_ts);
				return commit_ts;
			}

		private:
			TransactionManager(const TransactionManager &);
			TransactionManager& operator=(const TransactionManager &);

		protected:
			BaseStorageManager *const storage_manager_;
			BaseLogger *const logger_;
			size_t thread_id_;
			size_t thread_count_;
			size_t table_count_;
			uint64_t start_timestamp_;
			bool is_first_access_;
			uint64_t global_ts_;
			uint32_t local_ts_;
			AccessList<kMaxAccessNum> access_list_;
			InsertionList<kMaxAccessNum> insertion_list_;
			TableRecords *t_records_;

#if defined(BATCH_TIMESTAMP)
			BatchTimestamp batch_ts_;
#endif
#if defined(SILO)
			WritePtrList<kMaxAccessNum> write_list_;
#endif
#if defined(HEALING)
			AccessPtrList<kMaxAccessPerTableNum> *access_caches_;
			AccessPtrsList<kMaxOptPerTableNum, kMaxAccessPerOptNum> *accesses_caches_;
			AccessList<kMaxAccessPerTableNum> *access_lists_;
			InsertionList<kMaxAccessPerTableNum> *insertion_lists_;
#else
#endif
#if defined(MVTO) || defined(MVLOCK) || defined(MVLOCK_WAIT) || defined(MVOCC)
			std::vector<SchemaRecord*> read_only_set_;
#endif
#if defined(DBX)
			RtmLock *rtm_lock_;
#endif
		};
	}
}

#endif
