#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_ATOMIC_PROCEDURES_ORDER_STATUS_PROCEDURE_H__
#define __CAVALIA_TPCC_BENCHMARK_ATOMIC_PROCEDURES_ORDER_STATUS_PROCEDURE_H__

#include <Transaction/StoredProcedure.h>
#include "../TpccInformation.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Tpcc{
			namespace AtomicProcedures{
				class OrderStatusProcedure : public StoredProcedure{
				public:
					OrderStatusProcedure(const size_t &txn_type) : StoredProcedure(txn_type){
						context_.is_read_only_ = true;
						order_line_records = new SchemaRecords(15);
					}
					virtual ~OrderStatusProcedure(){}

					virtual bool Execute(TxnParam *param, CharArray &ret, const ExeContext &exe_context){
						context_.PassContext(exe_context);
						const OrderStatusParam *order_status_param = static_cast<const OrderStatusParam*>(param);
						
						if (order_status_param->c_id_ == -1){
							// "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST"
						}
						else{
							// "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?"
						}

						memcpy(o_key, &order_status_param->c_id_, sizeof(int));
						memcpy(o_key + sizeof(int), &order_status_param->d_id_, sizeof(int));
						memcpy(o_key + sizeof(int)+sizeof(int), &order_status_param->w_id_, sizeof(int));
						SchemaRecord *order_record = NULL;
						// "getLastOrder": "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1"
						DB_QUERY(SelectRecord(&context_, ORDER_TABLE_ID, 0, std::string(o_key, sizeof(int)* 3), order_record, READ_ONLY));
						
						if (order_record != NULL){
							int o_id = *(char*)(order_record->GetColumn(0));
							memcpy(ol_key, &o_id, sizeof(o_id));
							memcpy(ol_key + sizeof(int), &order_status_param->d_id_, sizeof(int));
							memcpy(ol_key + sizeof(int)+sizeof(int), &order_status_param->w_id_, sizeof(int));
							order_line_records->Clear();
							// "getOrderLines": "SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?"
							DB_QUERY(SelectRecords(&context_, ORDER_LINE_TABLE_ID, 0, std::string(ol_key, sizeof(int)*3), order_line_records, READ_ONLY));
							for (size_t i = 0; i < order_line_records->curr_size_; ++i){
								SchemaRecord *ol_record = order_line_records->records_[i];
								int i_id = *(int*)(ol_record->GetColumn(4));
								ret.Memcpy(ret.size_, (char*)(&i_id), sizeof(i_id));
								ret.size_ += sizeof(i_id);
							}
						}
						else{
							assert(false);
						}
						return transaction_manager_->CommitTransaction(&context_, param, ret);
					}

				private:
					OrderStatusProcedure(const OrderStatusProcedure&);
					OrderStatusProcedure& operator=(const OrderStatusProcedure&);

				private:
					char o_key[sizeof(int)* 3];
					char ol_key[sizeof(int)* 3];
					SchemaRecords *order_line_records;

				};
			}
		}
	}
}

#endif
