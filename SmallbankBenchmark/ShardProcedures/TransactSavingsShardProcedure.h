#pragma once
#ifndef __CAVALIA_SMALLBANK_BENCHMARK_SHARD_PROCEDURES_TRANSACT_SAVINGS_PROCEDURE_H__
#define __CAVALIA_SMALLBANK_BENCHMARK_SHARD_PROCEDURES_TRANSACT_SAVINGS_PROCEDURE_H__

#include <Transaction/StoredProcedure.h>
#include "../SmallbankInformation.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Smallbank{
			namespace ShardProcedures{
				class TransactSavingsShardProcedure : public StoredProcedure{
				public:
					TransactSavingsShardProcedure(const size_t &txn_type) : StoredProcedure(txn_type){}
					virtual ~TransactSavingsShardProcedure(){}

					virtual bool Execute(TxnParam *param, CharArray &ret, const ExeContext &exe_context){
						const TransactSavingsParam* ts_param = static_cast<const TransactSavingsParam*>(param);
						size_t part_id = (ts_param->custid_ - 1) % partition_count_;
						SchemaRecord *cust_record = NULL;
						DB_QUERY(SelectKeyRecord(&context_, ACCOUNTS_TABLE_ID, part_id, std::string((char*)(&ts_param->custid_), sizeof(int64_t)), cust_record, READ_ONLY));
						assert(cust_record != NULL);
						SchemaRecord *savings_record = NULL;
						DB_QUERY(SelectKeyRecord(&context_, SAVINGS_TABLE_ID, part_id, std::string((char*)(&ts_param->custid_), sizeof(int64_t)), savings_record, READ_WRITE));
						assert(savings_record != NULL);
						float cur_savings = *(float*)(savings_record->GetColumn(1));
						//if (cur_savings < ts_param->amount_){
						//	//transaction_manager_->AbortTransaction(txn_id);
						//	return transaction_manager_->CommitTransaction(&context_, param, NULL);
						//}
						float final_savings = cur_savings - ts_param->amount_;
						savings_record->UpdateColumn(1, (char*)(&final_savings));

						ret.Memcpy(ret.size_, (char*)(&ts_param->custid_), sizeof(ts_param->custid_));
						ret.size_ += sizeof(ts_param->custid_);
						ret.Memcpy(ret.size_, (char*)(&final_savings), sizeof(final_savings));
						ret.size_ += sizeof(final_savings);
						return transaction_manager_->CommitTransaction(&context_, param, ret);
					}

				private:
					TransactSavingsShardProcedure(const TransactSavingsShardProcedure&);
					TransactSavingsShardProcedure& operator=(const TransactSavingsShardProcedure&);

					char ret_str_[32];
				};
			}
		}
	}
}

#endif
