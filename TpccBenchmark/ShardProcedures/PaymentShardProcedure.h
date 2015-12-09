#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_ATOMIC_PROCEDURES_PAYMENT_SHARD_PROCEDURE_H__
#define __CAVALIA_TPCC_BENCHMARK_ATOMIC_PROCEDURES_PAYMENT_SHARD_PROCEDURE_H__

#include <Transaction/StoredProcedure.h>
#include "../TpccInformation.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Tpcc{
			namespace ShardProcedures{
				class PaymentShardProcedure : public StoredProcedure{
				public:
					PaymentShardProcedure(const size_t &txn_type) : StoredProcedure(txn_type){}
					virtual ~PaymentShardProcedure(){}

					virtual bool Execute(TxnParam *param, CharArray &ret, const ExeContext &exe_context){
						const PaymentParam *payment_param = static_cast<const PaymentParam*>(param);
						int partition_id = (payment_param->w_id_ - 1) % partition_count_;
						SchemaRecord *warehouse_record = NULL;
						// "getWarehouse": "SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = ?"
						// "updateWarehouseBalance": "UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?"
						DB_QUERY(SelectKeyRecord(&context_, WAREHOUSE_TABLE_ID, partition_id, std::string((char*)(&payment_param->w_id_), sizeof(int)), warehouse_record, READ_WRITE));
						double w_ytd = *(double*)(warehouse_record->GetColumn(8));
						ret.Memcpy(ret.size_, (char*)(&w_ytd), sizeof(w_ytd));
						ret.size_ += sizeof(w_ytd);
						double new_w_ytd = w_ytd + payment_param->h_amount_;
						warehouse_record->UpdateColumn(8, (char*)(&new_w_ytd));
						memcpy(d_key, &payment_param->d_id_, sizeof(int));
						memcpy(d_key + sizeof(int), &payment_param->w_id_, sizeof(int));

						SchemaRecord *district_record = NULL;
						// "getDistrict": "SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?"
						// "updateDistrictBalance": "UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE D_W_ID  = ? AND D_ID = ?"
						DB_QUERY(SelectKeyRecord(&context_, DISTRICT_TABLE_ID, partition_id, std::string(d_key, sizeof(int)* 2), district_record, READ_WRITE));
						double d_ytd = *(double*)(district_record->GetColumn(9));
						ret.Memcpy(ret.size_, (char*)(&d_ytd), sizeof(d_ytd));
						ret.size_ += sizeof(d_ytd);
						double new_d_ytd = d_ytd + payment_param->h_amount_;
						district_record->UpdateColumn(9, (char*)(&new_d_ytd));

						SchemaRecord *customer_record = NULL;
						if (payment_param->c_id_ == -1){
							assert(false);
							memcpy(cname_key, &payment_param->d_id_, sizeof(int));
							memcpy(cname_key + sizeof(int), &payment_param->w_id_, sizeof(int));
							//
							// "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST"

						}
						else{
							memcpy(c_key, &payment_param->c_id_, sizeof(int));
							memcpy(c_key + sizeof(int), &payment_param->d_id_, sizeof(int));
							memcpy(c_key + sizeof(int)+sizeof(int), &payment_param->w_id_, sizeof(int));
							// "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?"
							DB_QUERY(SelectKeyRecord(&context_, CUSTOMER_TABLE_ID, partition_id, std::string(c_key, sizeof(int)* 3), customer_record, READ_WRITE));
						}
						// "updateBCCustomer": "UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ?, C_DATA = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?"
						// "updateGCCustomer": "UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?"
						double balance = *(double*)(customer_record->GetColumn(16)) - payment_param->h_amount_;
						customer_record->UpdateColumn(16, (char*)(&balance));
						double ytd_payment = *(double*)(customer_record->GetColumn(17)) + payment_param->h_amount_;
						customer_record->UpdateColumn(17, (char*)(&ytd_payment));
						int payment_cnt = *(int*)(customer_record->GetColumn(18)) + 1;
						customer_record->UpdateColumn(18, (char*)(&payment_cnt));

						char *history_data = MemAllocator::Alloc(TpccSchema::GenerateHistorySchema()->GetSchemaSize());
						SchemaRecord *history_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
						new(history_record)SchemaRecord(TpccSchema::GenerateHistorySchema(), history_data);
						history_record->SetColumn(0, (char*)(&payment_param->c_id_));
						history_record->SetColumn(1, (char*)(&payment_param->c_d_id_));
						history_record->SetColumn(2, (char*)(&payment_param->c_w_id_));
						history_record->SetColumn(3, (char*)(&payment_param->d_id_));
						history_record->SetColumn(4, (char*)(&payment_param->w_id_));
						history_record->SetColumn(5, (char*)(&payment_param->h_date_));
						history_record->SetColumn(6, (char*)(&payment_param->h_amount_));
						memcpy(h_key, &payment_param->c_id_, sizeof(int));
						memcpy(h_key + sizeof(int), &payment_param->w_id_, sizeof(int));
						memcpy(h_key + sizeof(int)+sizeof(int), &payment_param->h_date_, sizeof(int64_t));
						// "insertHistory": "INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
						DB_QUERY(InsertRecord(&context_, HISTORY_TABLE_ID, std::string(h_key, sizeof(int)+sizeof(int)+sizeof(int64_t)), history_record));

						return transaction_manager_->CommitTransaction(&context_, param, ret);
					}

				private:
					PaymentShardProcedure(const PaymentShardProcedure&);
					PaymentShardProcedure& operator=(const PaymentShardProcedure&);

				private:
					char d_key[sizeof(int)* 2];
					char c_key[sizeof(int)* 3];
					char cname_key[sizeof(int)+sizeof(int)+32];
					char h_key[sizeof(int)+sizeof(int)+sizeof(int64_t)];
				};
			}
		}
	}
}

#endif
