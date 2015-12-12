#pragma once
#ifndef __CAVALIA_SMALLBANK_BENCHMARK_SHARD_PROCEDURES_AMALGAMATE_PROCEDURE_H__
#define __CAVALIA_SMALLBANK_BENCHMARK_SHARD_PROCEDURES_AMALGAMATE_PROCEDURE_H__

#include <Transaction/StoredProcedure.h>
#include "../SmallbankInformation.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Smallbank{
			namespace ShardProcedures{
				class AmalgamateShardProcedure : public StoredProcedure{
				public:
					AmalgamateShardProcedure(const size_t &txn_type) : StoredProcedure(txn_type){}
					virtual ~AmalgamateShardProcedure(){}

					virtual bool Execute(TxnParam *param, CharArray &ret, const ExeContext &exe_context){
						const AmalgamateParam* amalgamate_param = static_cast<const AmalgamateParam*>(param);
						size_t part_id_0 = (amalgamate_param->custid_0_ - 1) % partition_count_;
						size_t part_id_1 = (amalgamate_param->custid_1_ - 1) % partition_count_;
						SchemaRecord *custid_0_record = NULL;
						DB_QUERY(SelectKeyRecord(&context_, ACCOUNTS_TABLE_ID, part_id_0, std::string((char*)(&amalgamate_param->custid_0_), sizeof(int64_t)), custid_0_record, READ_ONLY));
						SchemaRecord *custid_1_record = NULL;
						DB_QUERY(SelectKeyRecord(&context_, ACCOUNTS_TABLE_ID, part_id_1, std::string((char*)(&amalgamate_param->custid_1_), sizeof(int64_t)), custid_1_record, READ_ONLY));
						assert(custid_0_record != NULL);
						assert(custid_1_record != NULL);
						SchemaRecord *custid_0_savings_record = NULL;
						DB_QUERY(SelectKeyRecord(&context_, SAVINGS_TABLE_ID, part_id_0, std::string((char*)(&amalgamate_param->custid_0_), sizeof(int64_t)), custid_0_savings_record, READ_WRITE));
						SchemaRecord *custid_0_checking_record = NULL;
						DB_QUERY(SelectKeyRecord(&context_, CHECKING_TABLE_ID, part_id_0, std::string((char*)(&amalgamate_param->custid_0_), sizeof(int64_t)), custid_0_checking_record, READ_WRITE));
						assert(custid_0_savings_record != NULL);
						assert(custid_0_checking_record != NULL);
						float total = *(float*)(custid_0_savings_record->GetColumn(1)) + *(float*)(custid_0_checking_record->GetColumn(1));

						const float zero = 0.0;
						custid_0_checking_record->UpdateColumn(1, (char*)(&zero));
						custid_0_savings_record->UpdateColumn(1, (char*)(&zero));

						SchemaRecord *custid_1_checking_record = NULL;
						DB_QUERY(SelectKeyRecord(&context_, CHECKING_TABLE_ID, part_id_1, std::string((char*)(&amalgamate_param->custid_1_), sizeof(int64_t)), custid_1_checking_record, READ_WRITE));
						assert(custid_1_checking_record != NULL);
						const float checking_amount = *(float*)(custid_1_checking_record->GetColumn(1)) + total;
						custid_1_checking_record->UpdateColumn(1, (char*)(&checking_amount));
						return transaction_manager_->CommitTransaction(&context_, param, ret);
					}

				private:
				AmalgamateShardProcedure(const AmalgamateShardProcedure&);
				AmalgamateShardProcedure& operator=(const AmalgamateShardProcedure&);
				};
			}
		}
	}
}

#endif
