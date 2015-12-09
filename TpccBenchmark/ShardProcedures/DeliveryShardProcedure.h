#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_ATOMIC_PROCEDURES_DELIVERY_SHARD_PROCEDURE_H__
#define __CAVALIA_TPCC_BENCHMARK_ATOMIC_PROCEDURES_DELIVERY_SHARD_PROCEDURE_H__

#include <unordered_map>
#include <Transaction/StoredProcedure.h>
#include "../TpccInformation.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Tpcc{
			namespace ShardProcedures{

				class DeliveryShardProcedure : public StoredProcedure{
				public:
					DeliveryShardProcedure(const size_t &txn_type) : StoredProcedure(txn_type){
						order_line_records = new SchemaRecords(15);
					}
					virtual ~DeliveryShardProcedure(){}

					virtual bool Execute(TxnParam *param, CharArray &ret, const ExeContext &exe_context){
						const DeliveryParam *delivery_param = static_cast<const DeliveryParam*>(param);
						int partition_id = (delivery_param->w_id_ - 1) % partition_count_;
						for (int no_d_id = 1; no_d_id <= DISTRICTS_PER_WAREHOUSE; ++no_d_id){
							memcpy(d_no_key, &no_d_id, sizeof(int));
							memcpy(d_no_key + sizeof(int), &(delivery_param->w_id_), sizeof(int));
							SchemaRecord *district_new_order_record = NULL;
							// "getNewOrder": "SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID > -1 LIMIT 1"
							// "deleteNewOrder": "DELETE FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID = ?"
							DB_QUERY(SelectKeyRecord(&context_, DISTRICT_NEW_ORDER_TABLE_ID, partition_id, std::string(d_no_key, sizeof(int)* 2), district_new_order_record, READ_WRITE));
							assert(district_new_order_record != NULL);
							int no_o_id = *(int*)(district_new_order_record->GetColumn(2));
							memcpy(no_key, &no_o_id, sizeof(int));
							memcpy(no_key + sizeof(int), &no_d_id, sizeof(int));
							memcpy(no_key + sizeof(int)+sizeof(int), &(delivery_param->w_id_), sizeof(int));
							SchemaRecord *new_order_record = NULL;
							DB_QUERY(SelectKeyRecord(&context_, NEW_ORDER_TABLE_ID, partition_id, std::string(no_key, sizeof(int)* 3), new_order_record, READ_ONLY));
							if (new_order_record != NULL){
								no_o_ids[no_d_id - 1] = no_o_id;
								int next_o_id = no_o_id + 1;
								district_new_order_record->UpdateColumn(2, (char*)(&next_o_id));
							}
							else{
								no_o_ids[no_d_id - 1] = -1;
								// TODO: this place should be modified after implementing an efficient index.
								int next_o_id = 1;
								district_new_order_record->UpdateColumn(2, (char*)(&next_o_id));
							}
						}

						for (size_t no_d_id = 1; no_d_id <= DISTRICTS_PER_WAREHOUSE; ++no_d_id){
							if (no_o_ids[no_d_id - 1] == -1){
								continue;
							}
							memcpy(o_key, &(no_o_ids[no_d_id - 1]), sizeof(int));
							memcpy(o_key + sizeof(int), &(no_d_id), sizeof(int));
							memcpy(o_key + sizeof(int)+sizeof(int), &(delivery_param->w_id_), sizeof(int));
							SchemaRecord *order_record = NULL;
							// "updateOrderLine": "UPDATE ORDER_LINE SET OL_DELIVERY_D = ? WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?"
							// "sumOLAmount": "SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?"
							DB_QUERY(SelectKeyRecord(&context_, ORDER_TABLE_ID, partition_id, std::string(o_key, sizeof(int)* 3), order_record, READ_WRITE));
							assert(order_record != NULL);
							order_record->UpdateColumn(5, (char*)(&delivery_param->o_carrier_id_));
							int c_id = *(int*)(order_record->GetColumn(1));
							c_ids[no_d_id - 1] = c_id;
						}

						for (size_t no_d_id = 1; no_d_id <= DISTRICTS_PER_WAREHOUSE; ++no_d_id){
							if (no_o_ids[no_d_id - 1] == -1){
								continue;
							}
							//memcpy(ol_key, &(no_o_ids[no_d_id - 1]), sizeof(int));
							//memcpy(ol_key + sizeof(int), &no_d_id, sizeof(int));
							//memcpy(ol_key + sizeof(int)+sizeof(int), &delivery_param->w_id_, sizeof(int));
							//order_line_records->Clear();
							//// "updateOrderLine": "UPDATE ORDER_LINE SET OL_DELIVERY_D = ? WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?"
							//// "sumOLAmount": "SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?"
							//DB_QUERY(SelectRecords(&context_, ORDER_LINE_TABLE_ID, partition_id, 0, std::string(ol_key, sizeof(int)* 3), order_line_records, READ_WRITE));
							//assert(order_line_records != NULL);
							//double sum = 0;
							//for (size_t i = 0; i < order_line_records->curr_size_; ++i) {
							//	SchemaRecord *ol_record = order_line_records->records_[i];
							//	assert(ol_record != NULL);
							//	ol_record->UpdateColumn(6, (char*)(&delivery_param->ol_delivery_d_));
							//	sum += *(double*)(ol_record->GetColumn(8));
							//}
							//sums[no_d_id - 1] = sum;
							sums[no_d_id - 1] = 1;
						}

						for (size_t no_d_id = 1; no_d_id <= DISTRICTS_PER_WAREHOUSE; ++no_d_id){
							if (no_o_ids[no_d_id - 1] == -1){
								continue;
							}
							memcpy(c_key, &(c_ids[no_d_id - 1]), sizeof(int));
							memcpy(c_key + sizeof(int), &no_d_id, sizeof(int));
							memcpy(c_key + sizeof(int)+sizeof(int), &(delivery_param->w_id_), sizeof(int));
							SchemaRecord *customer_record = NULL;
							// "updateCustomer": "UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + ? WHERE C_ID = ? AND C_D_ID = ? AND C_W_ID = ?"
							DB_QUERY(SelectKeyRecord(&context_, CUSTOMER_TABLE_ID, partition_id, std::string(c_key, sizeof(int)* 3), customer_record, READ_WRITE));
							assert(customer_record != NULL);
							double balance = *(const double*)(customer_record->GetColumn(16)) + sums[no_d_id - 1];
							customer_record->UpdateColumn(16, (char*)(&balance));
						}

						for (size_t no_d_id = 1; no_d_id <= DISTRICTS_PER_WAREHOUSE; ++no_d_id){
							ret.Memcpy(ret.size_, (char*)(&no_o_ids[no_d_id - 1]), sizeof(int));
							ret.size_ += sizeof(int);
							ret.Memcpy(ret.size_, (char*)(&no_d_id), sizeof(int));
							ret.size_ += sizeof(int);
						}
						return transaction_manager_->CommitTransaction(&context_, param, ret);
					}

				private:
					DeliveryShardProcedure(const DeliveryShardProcedure&);
					DeliveryShardProcedure& operator=(const DeliveryShardProcedure&);

				private:
					char d_no_key[sizeof(int)+sizeof(int)];
					char no_key[sizeof(int)+sizeof(int)+sizeof(int)];
					char ol_key[sizeof(int)+sizeof(int)+sizeof(int)];
					char o_key[sizeof(int)+sizeof(int)+sizeof(int)];
					char c_key[sizeof(int)+sizeof(int)+sizeof(int)];

					int no_o_ids[DISTRICTS_PER_WAREHOUSE];
					double sums[DISTRICTS_PER_WAREHOUSE];
					int c_ids[DISTRICTS_PER_WAREHOUSE];
					SchemaRecords *order_line_records;
				};
			}
		}
	}
}

#endif
