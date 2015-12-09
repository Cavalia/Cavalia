#if defined(ST)
#include "TransactionManager.h"

namespace Cavalia{
	namespace Database{
		bool TransactionManager::InsertRecord(TxnContext *context, const size_t &table_id, const std::string &primary_key, SchemaRecord *record){
			//storage_manager_->tables_[table_id]->InsertRecord(primary_key, record);
			return true;
		}

		bool TransactionManager::SelectRecordCC(TxnContext *context, const size_t &table_id, TableRecord *t_record, SchemaRecord *&record, const AccessType access_type) {
			record = t_record->record_;
			return true;
		}

		bool TransactionManager::CommitTransaction(TxnContext *context, TxnParam *param, CharArray &ret_str){
			return true;
		}

		void TransactionManager::AbortTransaction() {}
	}
}

#endif
