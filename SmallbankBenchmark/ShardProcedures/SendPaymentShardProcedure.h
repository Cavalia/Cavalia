#pragma once
#ifndef __CAVALIA_SMALLBANK_BENCHMARK_SHARD_PROCEDURES_SEND_PAYMENT_PROCEDURE_H__
#define __CAVALIA_SMALLBANK_BENCHMARK_SHARD_PROCEDURES_SEND_PAYMENT_PROCEDURE_H__

#include <Transaction/StoredProcedure.h>
#include "../SmallbankInformation.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Smallbank{
			namespace ShardProcedures{
				class SendPaymentShardProcedure : public StoredProcedure{
				public:
					SendPaymentShardProcedure(const size_t &txn_type) : StoredProcedure(txn_type){}
					virtual ~SendPaymentShardProcedure(){}

					virtual bool Execute(TxnParam *param, CharArray &ret, const ExeContext &exe_context){
						const SendPaymentParam* sp_param = static_cast<const SendPaymentParam*>(param);
						size_t part_id_0 = (sp_param->custid_0_ - 1) % partition_count_;
						size_t part_id_1 = (sp_param->custid_1_ - 1) % partition_count_;
						SchemaRecord *custid_0_record = NULL;
						DB_QUERY(SelectKeyRecord(&context_, ACCOUNTS_TABLE_ID, part_id_0, std::string((char*)(&sp_param->custid_0_), sizeof(int64_t)), custid_0_record, READ_ONLY));
						SchemaRecord *custid_1_record = NULL;
						DB_QUERY(SelectKeyRecord(&context_, ACCOUNTS_TABLE_ID, part_id_1, std::string((char*)(&sp_param->custid_1_), sizeof(int64_t)), custid_1_record, READ_ONLY));
						assert(custid_0_record != NULL);
						assert(custid_1_record != NULL);
						SchemaRecord *sendacct_checking_record = NULL;
						DB_QUERY(SelectKeyRecord(&context_, CHECKING_TABLE_ID, part_id_0, std::string((char*)(&sp_param->custid_0_), sizeof(int64_t)), sendacct_checking_record, READ_WRITE));
						assert(sendacct_checking_record != NULL);
						SchemaRecord *destacct_checking_record = NULL;
						DB_QUERY(SelectKeyRecord(&context_, CHECKING_TABLE_ID, part_id_1, std::string((char*)(&sp_param->custid_1_), sizeof(int64_t)), destacct_checking_record, READ_WRITE));
						assert(destacct_checking_record != NULL);
						float sendacct_checking = *(float*)(sendacct_checking_record->GetColumn(1));
						//if (sp_param->amount_ > sendacct_checking){
						//	//transaction_manager_->AbortTransaction(txn_id);
						//	return transaction_manager_->CommitTransaction(&context_, param, NULL);
						//}
						float sendacct_final_checking = sendacct_checking - sp_param->amount_;
						float destacct_final_checking = *(float*)(destacct_checking_record->GetColumn(1)) + sp_param->amount_;
						sendacct_checking_record->UpdateColumn(1, (char*)(&sendacct_final_checking));
						destacct_checking_record->UpdateColumn(1, (char*)(&destacct_final_checking));

						ret.Memcpy(ret.size_, (char*)(&sp_param->custid_0_), sizeof(sp_param->custid_0_));
						ret.size_ += sizeof(sp_param->custid_0_);
						ret.Memcpy(ret.size_, (char*)(&sp_param->custid_1_), sizeof(sp_param->custid_1_));
						ret.size_ += sizeof(sp_param->custid_1_);
						ret.Memcpy(ret.size_, (char*)(&sp_param->amount_), sizeof(sp_param->amount_));
						ret.size_ += sizeof(sp_param->amount_);

						return transaction_manager_->CommitTransaction(&context_, param, ret);
					}

				private:
					SendPaymentShardProcedure(const SendPaymentShardProcedure&);
					SendPaymentShardProcedure& operator=(const SendPaymentShardProcedure&);
				};
			}
		}
	}
}

#endif
