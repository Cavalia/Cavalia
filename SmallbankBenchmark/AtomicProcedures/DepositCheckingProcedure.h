#pragma once
#ifndef __CAVALIA_SMALLBANK_BENCHMARK_ATOMIC_PROCEDURES_DEPOSIT_CHECKING_PROCEDURE_H__
#define __CAVALIA_SMALLBANK_BENCHMARK_ATOMIC_PROCEDURES_DEPOSIT_CHECKING_PROCEDURE_H__

#include <Transaction/StoredProcedure.h>
#include "../SmallbankInformation.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Smallbank{
			namespace AtomicProcedures{
				class DepositCheckingProcedure : public StoredProcedure{
				public:
					DepositCheckingProcedure(const size_t &txn_type) : StoredProcedure(txn_type){}
					virtual ~DepositCheckingProcedure(){}

					virtual bool Execute(TxnParam *param, CharArray &ret, const ExeContext &exe_context){
						context_.PassContext(exe_context);
						const DepositCheckingParam * dc_param = static_cast<const DepositCheckingParam*>(param);
						SchemaRecord *cust_record = NULL;
						DB_QUERY(SelectKeyRecord(&context_, ACCOUNTS_TABLE_ID, std::string((char*)(&dc_param->custid_), sizeof(int64_t)), cust_record, READ_ONLY));
						assert(cust_record != NULL);
						SchemaRecord *checking_record = NULL;
						DB_QUERY(SelectKeyRecord(&context_, CHECKING_TABLE_ID, std::string((char*)(&dc_param->custid_), sizeof(int64_t)), checking_record, READ_WRITE));
						assert(checking_record != NULL);
						float final_amount = *(float*)(checking_record->GetColumn(1)) + dc_param->amount_;
						checking_record->UpdateColumn(1, (char*)(&final_amount));
						ret.Memcpy(ret.size_, (char*)(&dc_param->custid_), sizeof(dc_param->custid_));
						ret.size_ += sizeof(dc_param->custid_);
						ret.Memcpy(ret.size_, (char*)(&final_amount), sizeof(final_amount));
						ret.size_ += sizeof(final_amount);
						return transaction_manager_->CommitTransaction(&context_, param, ret);
					}

				private:
					DepositCheckingProcedure(const DepositCheckingProcedure&);
					DepositCheckingProcedure& operator=(const DepositCheckingProcedure&);
				};
			}
		}
	}
}

#endif
