#pragma once
#ifndef __CAVALIA_SMALLBANK_BENCHMARK_SHARD_PROCEDURES_WRITE_CHECK_PROCEDURE_H__
#define __CAVALIA_SMALLBANK_BENCHMARK_SHARD_PROCEDURES_WRITE_CHECK_PROCEDURE_H__

#include <Transaction/StoredProcedure.h>
#include "../SmallbankInformation.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Smallbank{
			namespace ShardProcedures{
				class WriteCheckShardProcedure : public StoredProcedure{
				public:
					WriteCheckShardProcedure(const size_t &txn_type) : StoredProcedure(txn_type){}
					virtual ~WriteCheckShardProcedure(){}

					virtual bool Execute(TxnParam *param, CharArray &ret, const ExeContext &exe_context){
						const WriteCheckParam* wc_param = static_cast<const WriteCheckParam*>(param);
						size_t part_id = (wc_param->custid_ - 1) % partition_count_;
						SchemaRecord  *cust_record = NULL;
						DB_QUERY(SelectKeyRecord(&context_, ACCOUNTS_TABLE_ID, part_id, std::string((char*)(&wc_param->custid_), sizeof(int64_t)), cust_record, READ_ONLY));
						assert(cust_record != NULL);
						SchemaRecord *savings_record = NULL;
						DB_QUERY(SelectKeyRecord(&context_, SAVINGS_TABLE_ID, part_id, std::string((char*)(&wc_param->custid_), sizeof(int64_t)), savings_record, READ_ONLY));
						SchemaRecord *checking_record = NULL;
						DB_QUERY(SelectKeyRecord(&context_, CHECKING_TABLE_ID, part_id, std::string((char*)(&wc_param->custid_), sizeof(int64_t)), checking_record, READ_WRITE));
						assert(savings_record != NULL);
						assert(checking_record != NULL);
						float balance = *(float*)(savings_record->GetColumn(1)) + *(float*)(checking_record->GetColumn(1));
						float final_checking = 0.0;
						if (balance < wc_param->amount_) {
							final_checking = balance - (wc_param->amount_ + 1); // maybe negative 
							checking_record->UpdateColumn(1, (char*)(&final_checking));
						}
						else{
							final_checking = balance - wc_param->amount_;
							checking_record->UpdateColumn(1, (char*)(&final_checking));
						}
						ret.Memcpy(ret.size_, (char*)(&wc_param->custid_), sizeof(wc_param->custid_));
						ret.size_ += sizeof(wc_param->custid_);
						ret.Memcpy(ret.size_, (char*)(&final_checking), sizeof(final_checking));
						ret.size_ += sizeof(final_checking);
						return transaction_manager_->CommitTransaction(&context_, param, ret);
					}

				private:
					WriteCheckShardProcedure(const WriteCheckShardProcedure&);
					WriteCheckShardProcedure& operator=(const WriteCheckShardProcedure&);
				};
			}
		}
	}
}

#endif
