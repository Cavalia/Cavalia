#if defined(DELAY)
#include "TransactionManager.h"

namespace Cavalia {
	namespace Database {
		bool TransactionManager::InsertRecord(TxnContext *context, const size_t &table_id, const std::string &primary_key, SchemaRecord *record) {
			BEGIN_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			Insertion *insertion = insertion_lists_[table_id].NewInsertion();
			insertion->insertion_record_ = record;
			insertion->table_id_ = table_id;
			END_PHASE_MEASURE(thread_id_, INSERT_PHASE);
			return true;
		}

		bool TransactionManager::SelectRecordCC(TxnContext *context, const size_t &table_id, SchemaRecord *&record, const AccessType access_type) {
			assert(false);
			return false;
		}

		bool TransactionManager::SelectRecordCC(TxnContext *context, const size_t &table_id, SchemaRecord *&record, const AccessType access_type, const size_t &access_id, bool is_key_access) {
			return false;
		}

		bool TransactionManager::CommitTransaction(TxnContext *context, EventTuple *param, CharArray &ret_str) {
			return true;
		}
	}
}
#endif