#pragma once
#ifndef __CAVALIA_SMALLBANK_BENCHMARK_ATOMIC_PROCEDURES_BALANCE_PROCEDURE_H__
#define __CAVALIA_SMALLBANK_BENCHMARK_ATOMIC_PROCEDURES_BALANCE_PROCEDURE_H__

#include <Transaction/StoredProcedure.h>
#include "../SmallbankInformation.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Smallbank{
			namespace AtomicProcedures{
				class BalanceProcedure : public StoredProcedure{
				public:
					BalanceProcedure(const size_t &txn_type) : StoredProcedure(txn_type){
						context_.is_read_only_ = true;
					}
					virtual ~BalanceProcedure(){}

					virtual bool Execute(TxnParam *param, CharArray &ret, const ExeContext &exe_context){
						context_.PassContext(exe_context);
						const BalanceParam* balance_param = static_cast<const BalanceParam*>(param);
						SchemaRecord *custid_record = NULL;
						DB_QUERY(SelectKeyRecord(&context_, ACCOUNTS_TABLE_ID, std::string((char*)(&balance_param->custid_), sizeof(int64_t)), custid_record, READ_ONLY));
						assert(custid_record != NULL);
						SchemaRecord *savings_record = NULL;
						DB_QUERY(SelectKeyRecord(&context_, SAVINGS_TABLE_ID, std::string((char*)(&balance_param->custid_), sizeof(int64_t)), savings_record, READ_ONLY));
						assert(savings_record != NULL);
						SchemaRecord *checking_record = NULL;
						DB_QUERY(SelectKeyRecord(&context_, CHECKING_TABLE_ID, std::string((char*)(&balance_param->custid_), sizeof(int64_t)), checking_record, READ_ONLY));
						assert(checking_record != NULL);
						float total = *(float*)(savings_record->GetColumn(1)) + *(float*)(checking_record->GetColumn(1));
						ret.Memcpy(ret.size_, (char*)(&total), sizeof(float));
						ret.size_ += sizeof(float);
						return transaction_manager_->CommitTransaction(&context_, param, ret);
					}

				private:
					BalanceProcedure(const BalanceProcedure&);
					BalanceProcedure& operator=(const BalanceProcedure&);

				};
			}
		}
	}
}

#endif
