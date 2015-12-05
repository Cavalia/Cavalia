#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_ATOMIC_PROCEDURES_NEW_ORDER_SHARD_PROCEDURE_H__
#define __CAVALIA_TPCC_BENCHMARK_ATOMIC_PROCEDURES_NEW_ORDER_SHARD_PROCEDURE_H__

#include <Transaction/StoredProcedure.h>
#include "../TpccInformation.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Tpcc{
			namespace ShardProcedures{
				class NewOrderShardProcedure : public StoredProcedure{
				public:
					NewOrderShardProcedure(const size_t &txn_type) : StoredProcedure(txn_type){
						ol_amounts = (double*) MemAllocator::Alloc(15 * sizeof(double));
					}
					virtual ~NewOrderShardProcedure(){
						MemAllocator::Free((char*)ol_amounts);
					}

					virtual bool Execute(TxnParam *param, CharArray &ret, const ExeContext &exe_context){
						const NewOrderParam *new_order_param = static_cast<const NewOrderParam*>(param);
						int partition_id = (new_order_param->w_id_ - 1) % partition_count_;
						double total = 0;
						for (size_t i = 0; i < new_order_param->ol_cnt_; ++i){
							int item_id = new_order_param->i_ids_[i];
							SchemaRecord *item_record = NULL;
							// "getItemInfo": "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = ?"
							DB_QUERY(SelectKeyRecord(&context_, ITEM_TABLE_ID, partition_id, std::string((char*)(&item_id), sizeof(item_id)), item_record, READ_ONLY));
							// abort here!
							if (item_record == NULL){
								assert(false);
								transaction_manager_->AbortTransaction();
								return false;
							}
							double price = *(double*)(item_record->GetColumn(3));
							ret.Memcpy(ret.size_, (char*)(&item_id), sizeof(item_id));
							ret.size_ += sizeof(item_id);
							ret.Memcpy(ret.size_, (char*)(&price), sizeof(price));
							ret.size_ += sizeof(price);
							int ol_quantity = new_order_param->i_qtys_[i];
							double ol_amount = ol_quantity * price;
							ol_amounts[i] = ol_amount;
							total += ol_amount;
						}

						for (size_t i = 0; i < new_order_param->ol_cnt_; ++i){
							int ol_i_id = new_order_param->i_ids_[i];
							int ol_supply_w_id = new_order_param->i_w_ids_[i];
							memcpy(s_key, &ol_i_id, sizeof(int));
							memcpy(s_key + sizeof(int), &ol_supply_w_id, sizeof(int));
							SchemaRecord *stock_record = NULL;
							// "getStockInfo": "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_%02d FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?"
							// "updateStock": "UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? WHERE S_I_ID = ? AND S_W_ID = ?"
							DB_QUERY(SelectKeyRecord(&context_, STOCK_TABLE_ID, (ol_supply_w_id - 1) % partition_count_, std::string(s_key, sizeof(int)* 2), stock_record, READ_WRITE));
							assert(stock_record != NULL);
							int ol_quantity = new_order_param->i_qtys_[i];
							int ytd = *(int*)(stock_record->GetColumn(13)) + ol_quantity;
							stock_record->UpdateColumn(13, (char*)(&ytd));
							int quantity = *(int*)(stock_record->GetColumn(2));
							if (quantity >= ol_quantity + 10){
								quantity -= ol_quantity;
								stock_record->UpdateColumn(2, (char*)(&quantity));
							}
							else{
								quantity = quantity + 91 - ol_quantity;
								stock_record->UpdateColumn(2, (char*)(&quantity));
							}
							int order_cnt = *(int*)(stock_record->GetColumn(14)) + 1;
							stock_record->UpdateColumn(14, (char*)(&order_cnt));
							if (ol_supply_w_id != new_order_param->w_id_){
								int remote_cnt = *(int*)(stock_record->GetColumn(15)) + 1;
								stock_record->UpdateColumn(15, (char*)(&remote_cnt));
							}
							int dist_column = new_order_param->d_id_ + 2;
							stock_record->GetColumn(dist_column, s_dists[i]);
						}

						SchemaRecord *warehouse_record = NULL;
						// "getWarehouseTaxRate": "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = ?"
						DB_QUERY(SelectKeyRecord(&context_, WAREHOUSE_TABLE_ID, partition_id, std::string((char*)(&new_order_param->w_id_), sizeof(int)), warehouse_record, READ_ONLY));
						assert(warehouse_record != NULL);
						double w_tax = *(double*)(warehouse_record->GetColumn(7));

						memcpy(d_key, &(new_order_param->d_id_), sizeof(int));
						memcpy(d_key + sizeof(int), &(new_order_param->w_id_), sizeof(int));
						SchemaRecord *district_record = NULL;
						// "getDistrict": "SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = ? AND D_W_ID = ?"
						// "incrementNextOrderId": "UPDATE DISTRICT SET D_NEXT_O_ID = ? WHERE D_ID = ? AND D_W_ID = ?"
						DB_QUERY(SelectKeyRecord(&context_, DISTRICT_TABLE_ID, partition_id, std::string(d_key, sizeof(int)* 2), district_record, READ_WRITE));
						assert(district_record != NULL);
						int d_next_o_id = *(int*)(district_record->GetColumn(10));
						ret.Memcpy(ret.size_, (char*)(&d_next_o_id), sizeof(d_next_o_id));
						ret.size_ += sizeof(d_next_o_id);
						double d_tax = *(double*)(district_record->GetColumn(8));
						int o_id = d_next_o_id + 1;
						district_record->UpdateColumn(10, (char*)(&o_id));

						memcpy(c_key, &new_order_param->c_id_, sizeof(int));
						memcpy(c_key + sizeof(int), &new_order_param->d_id_, sizeof(int));
						memcpy(c_key + sizeof(int)+sizeof(int), &new_order_param->w_id_, sizeof(int));
						SchemaRecord *customer_record = NULL;
						// "getCustomer": "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?"
						DB_QUERY(SelectKeyRecord(&context_, CUSTOMER_TABLE_ID, partition_id, std::string(c_key, sizeof(int)* 3), customer_record, READ_ONLY));
						assert(customer_record != NULL);
						double c_discount = *(double*)(customer_record->GetColumn(15));

						char *new_order_data = MemAllocator::Alloc(TpccSchema::GenerateNewOrderSchema()->GetSchemaSize());
						SchemaRecord *new_order_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
						new(new_order_record)SchemaRecord(TpccSchema::GenerateNewOrderSchema(), new_order_data);
						new_order_record->SetColumn(0, (char*)(&d_next_o_id));
						new_order_record->SetColumn(1, (char*)(&new_order_param->d_id_));
						new_order_record->SetColumn(2, (char*)(&new_order_param->w_id_));
						memcpy(o_key, &d_next_o_id, sizeof(int));
						memcpy(o_key + sizeof(int), &(new_order_param->d_id_), sizeof(int));
						memcpy(o_key + sizeof(int)+sizeof(int), &(new_order_param->w_id_), sizeof(int));
						// "createNewOrder": "INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (?, ?, ?)"
						DB_QUERY(InsertRecord(&context_, NEW_ORDER_TABLE_ID, std::string(o_key, sizeof(int)* 3), new_order_record));

						bool all_local = true;
						for (auto & w_id : new_order_param->i_w_ids_){
							all_local = (all_local && (new_order_param->w_id_ == w_id));
						}
						char *order_data = MemAllocator::Alloc(TpccSchema::GenerateOrderSchema()->GetSchemaSize());
						SchemaRecord *order_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
						new(order_record)SchemaRecord(TpccSchema::GenerateOrderSchema(), order_data);
						order_record->SetColumn(0, (char*)(&d_next_o_id));
						order_record->SetColumn(1, (char*)(&new_order_param->c_id_));
						order_record->SetColumn(2, (char*)(&new_order_param->d_id_));
						order_record->SetColumn(3, (char*)(&new_order_param->w_id_));
						order_record->SetColumn(4, (char*)(&new_order_param->o_entry_d_));
						order_record->SetColumn(5, (char*)(&NULL_CARRIER_ID));
						order_record->SetColumn(6, (char*)(&new_order_param->ol_cnt_));
						order_record->SetColumn(7, (char*)(&all_local));
						// "createOrder": "INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
						DB_QUERY(InsertRecord(&context_, ORDER_TABLE_ID, std::string(o_key, sizeof(int)* 3), order_record));

						for (size_t i = 0; i < new_order_param->ol_cnt_; ++i){
							int ol_number = i + 1;
							int ol_i_id = new_order_param->i_ids_[i];
							int ol_supply_w_id = new_order_param->i_w_ids_[i];
							int ol_quantity = new_order_param->i_qtys_[i];
							char *order_line_data = MemAllocator::Alloc(TpccSchema::GenerateOrderLineSchema()->GetSchemaSize());
							SchemaRecord *order_line_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
							new(order_line_record)SchemaRecord(TpccSchema::GenerateOrderLineSchema(), order_line_data);
							order_line_record->SetColumn(0, (char*)(&d_next_o_id));
							order_line_record->SetColumn(1, (char*)(&new_order_param->d_id_));
							order_line_record->SetColumn(2, (char*)(&new_order_param->w_id_));
							order_line_record->SetColumn(3, (char*)(&ol_number));
							order_line_record->SetColumn(4, (char*)(&ol_i_id));
							order_line_record->SetColumn(5, (char*)(&ol_supply_w_id));
							order_line_record->SetColumn(6, (char*)(&new_order_param->o_entry_d_));
							order_line_record->SetColumn(7, (char*)(&ol_quantity));
							order_line_record->SetColumn(8, (char*)(&ol_amounts[i]));
							order_line_record->SetColumn(9, s_dists[i]);
							memcpy(ol_key, &d_next_o_id, sizeof(int));
							memcpy(ol_key + sizeof(int), &new_order_param->d_id_, sizeof(int));
							memcpy(ol_key + sizeof(int)+sizeof(int), &new_order_param->w_id_, sizeof(int));
							memcpy(ol_key + sizeof(int)+sizeof(int)+sizeof(int), &ol_number, sizeof(int));
							// "createOrderLine": "INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
							DB_QUERY(InsertRecord(&context_, ORDER_LINE_TABLE_ID, std::string(ol_key, sizeof(int)* 4), order_line_record));
						}

						ret.Memcpy(ret.size_, (char*)(&w_tax), sizeof(w_tax));
						ret.size_ += sizeof(w_tax);
						ret.Memcpy(ret.size_, (char*)(&d_tax), sizeof(d_tax));
						ret.size_ += sizeof(d_tax);
						ret.Memcpy(ret.size_, (char*)(&c_discount), sizeof(c_discount));
						ret.size_ += sizeof(c_discount);
						total *= (1 - c_discount)*(1 + w_tax + d_tax);
						ret.size_ += sizeof(total);

						return transaction_manager_->CommitTransaction(&context_, param, ret);
					}

				private:
					NewOrderShardProcedure(const NewOrderShardProcedure&);
					NewOrderShardProcedure& operator=(const NewOrderShardProcedure&);

				private:
					char d_key[sizeof(int)+sizeof(int)];
					char c_key[sizeof(int)+sizeof(int)+sizeof(int)];
					char s_key[sizeof(int)+sizeof(int)];
					char o_key[sizeof(int)+sizeof(int)+sizeof(int)];
					char ol_key[sizeof(int)+sizeof(int)+sizeof(int)+sizeof(int)];

					double *ol_amounts;
					std::string s_dists[15];
				};
			}
		}
	}
}

#endif
