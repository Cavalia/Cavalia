#pragma once
#ifndef __CAVALIA_DATABASE_STORED_PROCEDURE_H__
#define __CAVALIA_DATABASE_STORED_PROCEDURE_H__

#include <AllocatorHelper.h>
#include "../Storage/SchemaRecord.h"
#include "../Storage/SchemaRecords.h"
#include "TransactionManager.h"
#include "TxnContext.h"
#include "TxnParam.h"

#define DB_QUERY(statement) \
if (transaction_manager_->statement == false) return false;

#define DB_QUERY_CALLBACK(statement, callback) \
if (transaction_manager_->statement == false) { callback; return false; }

namespace Cavalia{
	namespace Database{
		class StoredProcedure{
		public:
			StoredProcedure(){
				context_.txn_type_ = 0;
			}
			StoredProcedure(const size_t &txn_type){
				context_.txn_type_ = txn_type;
			}
			virtual ~StoredProcedure(){}

			void SetStorageManager(BaseStorageManager *storage_manager) {
				storage_manager_ = storage_manager;
			}

			void SetTransactionManager(TransactionManager *transaction_manager){
				transaction_manager_ = transaction_manager;
			}

			void SetPartitionCount(const size_t &partition_count){
				partition_count_ = partition_count;
			}

			void SetPartitionId(const size_t &partition_id){
				partition_id_ = partition_id;
			}

			virtual bool Execute(TxnParam *param, CharArray &ret, const ExeContext &exe_context) { return true; }

		private:
			StoredProcedure(const StoredProcedure&);
			StoredProcedure& operator=(const StoredProcedure&);

		protected:
			TxnContext context_;
			BaseStorageManager *storage_manager_;
			TransactionManager *transaction_manager_;
			size_t partition_count_;
			size_t partition_id_;
		};
	}
}

#endif
