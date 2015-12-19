#pragma once
#ifndef __CAVALIA_DATABASE_TRANSACTION_MANAGER_H__
#define __CAVALIA_DATABASE_TRANSACTION_MANAGER_H__

#include <list>
#include <AllocatorHelper.h>
#include "../Meta/MetaTypes.h"
#include "../Profiler/Profilers.h"
#include "../Logger/ValueLogger.h"
#include "../Logger/AccessLogger.h"
#include "../Logger/CommandLogger.h"
#include "../Content/ContentCommon.h"
#include "../Storage/TableRecords.h"
#include "../Storage/SchemaRecords.h"
#include "../Storage/BaseStorageManager.h"
#include "TxnAccess.h"
#include "TxnContext.h"
#include "GlobalTimestamp.h"
#include "BatchTimestamp.h"
#include "Epoch.h"
#if defined(DBX) || defined(RTM) || defined(OCC_RTM) || defined(LOCK_RTM)
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
				local_epoch_ = 0;
				local_ts_ = 0;
				t_records_ = new TableRecords(64);
				
				// for multi-version concurrency-control schemes.
				this->progress_ts_ = 0;
				GlobalTimestamp::thread_timestamp_[thread_id_] = &(this->progress_ts_);
			}

			// for replayer.
			TransactionManager(BaseStorageManager *const storage_manager, BaseLogger *const logger) : storage_manager_(storage_manager), logger_(logger){}

			// destruction.
			virtual ~TransactionManager(){}

#if defined(DBX) || defined(RTM) || defined(OCC_RTM) || defined(LOCK_RTM)
			void SetRtmLock(RtmLock *rtm_lock){
				rtm_lock_ = rtm_lock;
			}
#endif

			bool InsertRecord(TxnContext *context, const size_t &table_id, const std::string &primary_key, SchemaRecord *record);
			bool InsertRecord(TxnContext *context, const size_t &table_id, SchemaRecord *record){
				return InsertRecord(context, table_id, record->GetPrimaryKey(), record);
			}

			// shared
			bool SelectKeyRecord(TxnContext *context, const size_t &table_id, const std::string &primary_key, SchemaRecord *&record, const AccessType access_type){
				BEGIN_INDEX_TIME_MEASURE(thread_id_);
				TableRecord *t_record = NULL;
				storage_manager_->tables_[table_id]->SelectKeyRecord(primary_key, t_record);
				END_INDEX_TIME_MEASURE(thread_id_);
				if (t_record != NULL){
					BEGIN_PHASE_MEASURE(thread_id_, SELECT_PHASE);
					bool rt = SelectRecordCC(context, table_id, t_record, record, access_type);
					END_PHASE_MEASURE(thread_id_, SELECT_PHASE);
					return rt;
				}
				else{
					//assert(false);
					// if no record is found, then a "virtual record" should be inserted as the placeholder so that we can lock it.
					return true;
				}
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
				else{
					//assert(false);
					// if no record is found, then a "virtual record" should be inserted as the placeholder so that we can lock it.
					return true;
				}
			}

			// shared
			bool SelectRecord(TxnContext *context, const size_t &table_id, const size_t &idx_id, const std::string &secondary_key, SchemaRecord *&record, const AccessType access_type){
				BEGIN_INDEX_TIME_MEASURE(thread_id_);
				TableRecord *t_record = NULL;
				storage_manager_->tables_[table_id]->SelectRecord(idx_id, secondary_key, t_record);
				END_INDEX_TIME_MEASURE(thread_id_);
				if (t_record != NULL){
					BEGIN_PHASE_MEASURE(thread_id_, SELECT_PHASE);
					bool rt = SelectRecordCC(context, table_id, t_record, record, access_type);
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
			bool SelectRecords(TxnContext *context, const size_t &table_id, const size_t &idx_id, const std::string &secondary_key, SchemaRecords *records, const AccessType access_type) {
				BEGIN_INDEX_TIME_MEASURE(thread_id_);
				storage_manager_->tables_[table_id]->SelectRecords(idx_id, secondary_key, t_records_);
				END_INDEX_TIME_MEASURE(thread_id_);
				BEGIN_PHASE_MEASURE(thread_id_, SELECT_PHASE);
				bool rt = SelectRecordsCC(context, table_id, t_records_, records, access_type);
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

			void CleanUp(){
#if defined(VALUE_LOGGING) || defined(COMMAND_LOGGING)
				logger_->CleanUp(this->thread_id_);
#endif
			}

		private:
			bool SelectRecordCC(TxnContext *context, const size_t &table_id, TableRecord *t_record, SchemaRecord *&s_record, const AccessType access_type);
			bool SelectRecordsCC(TxnContext *context, const size_t &table_id, TableRecords *t_records, SchemaRecords *records, const AccessType access_type){
				for (size_t i = 0; i < t_records->curr_size_; ++i) {
					SchemaRecord **s_record = &(records->records_[i]);
					TableRecord *t_record = t_records->records_[i];
					if (SelectRecordCC(context, table_id, t_record, *s_record, access_type) == false) {
						return false;
					}
				}
				return true;
			}

			uint64_t GenerateScalableTimestamp(const uint64_t &curr_epoch, const uint64_t &max_rw_ts){
				uint64_t max_global_ts = max_rw_ts >> 32;
				uint32_t max_local_ts = max_rw_ts & 0xFFFFFFFF;
				assert(curr_epoch >= max_global_ts);
				assert(curr_epoch >= this->local_epoch_);
				// init.
				if (curr_epoch > this->local_epoch_) {
					this->local_epoch_ = curr_epoch;
					this->local_ts_ = this->thread_id_;
				}
				assert(this->local_epoch_ == curr_epoch);
				// compute commit timestamp.
				if (curr_epoch == max_global_ts) {
					if (this->local_ts_ <= max_local_ts) {
						this->local_ts_ = (max_local_ts / thread_count_ + 1)*thread_count_ + thread_id_;
						assert(this->local_ts_ > max_local_ts);
					}
					assert(this->local_ts_ > max_local_ts);
				}
				assert(this->local_epoch_ == max_global_ts && this->local_ts_ >= max_local_ts || this->local_epoch_ > max_global_ts);

				uint64_t commit_ts = (this->local_epoch_ << 32) | this->local_ts_;
				assert(commit_ts >= max_rw_ts);
				return commit_ts;
			}

			uint64_t GenerateMonotoneTimestamp(const uint64_t &curr_epoch, const uint64_t &monotone_ts){
				uint32_t lower_bits = monotone_ts & 0xFFFFFFFF;
				uint64_t commit_ts = (curr_epoch << 32) | lower_bits;
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
			uint64_t local_epoch_;
			uint32_t local_ts_;
			std::atomic<uint64_t> progress_ts_;
			AccessList<kMaxAccessNum> access_list_;
			TableRecords *t_records_;

#if defined(BATCH_TIMESTAMP)
			BatchTimestamp batch_ts_;
#endif
#if defined(SILO)
			// write set.
			AccessPtrList<kMaxAccessNum> write_list_;
#endif
#if defined(MVTO) || defined(MVLOCK) || defined(MVLOCK_WAIT) || defined(MVOCC)
			std::vector<SchemaRecord*> read_only_set_;
#endif
#if defined(DBX) || defined(RTM) || defined(OCC_RTM) || defined(LOCK_RTM)
			RtmLock *rtm_lock_;
			std::list<std::pair<TableRecord*, SchemaRecord*>> garbage_set_;
#if defined(OCC_RTM) || defined(LOCK_RTM)
			AccessList<kMaxAccessNum> hot_access_list_;
#endif
#endif
		};
	}
}

#endif
