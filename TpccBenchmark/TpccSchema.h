#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_TPCC_SCHEMA_H__
#define __CAVALIA_TPCC_BENCHMARK_TPCC_SCHEMA_H__

#include <Storage/RecordSchema.h>
#include "TpccMeta.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Tpcc{
			using namespace Cavalia::Database;
			class TpccSchema{
			public:
				static RecordSchema* GetTableSchema(const size_t &table_id){
					RecordSchema *schema_ptr = NULL;
					switch (table_id)
					{
					case ITEM_TABLE_ID:
						schema_ptr = TpccSchema::GenerateItemSchema();
						break;
					case STOCK_TABLE_ID:
						schema_ptr = TpccSchema::GenerateStockSchema();
						break;
					case WAREHOUSE_TABLE_ID:
						schema_ptr = TpccSchema::GenerateWarehouseSchema();
						break;
					case DISTRICT_TABLE_ID:
						schema_ptr = TpccSchema::GenerateDistrictSchema();
						break;
					case DISTRICT_NEW_ORDER_TABLE_ID:
						schema_ptr = TpccSchema::GenerateDistrictNewOrderSchema();
						break;
					case NEW_ORDER_TABLE_ID:
						schema_ptr = TpccSchema::GenerateNewOrderSchema();
						break;
					case ORDER_TABLE_ID:
						schema_ptr = TpccSchema::GenerateOrderSchema();
						break;
					case ORDER_LINE_TABLE_ID:
						schema_ptr = TpccSchema::GenerateOrderLineSchema();
						break;
					case CUSTOMER_TABLE_ID:
						schema_ptr = TpccSchema::GenerateCustomerSchema();
						break;
					case HISTORY_TABLE_ID:
						schema_ptr = TpccSchema::GenerateHistorySchema();
						break;
					default:
						assert(false);
						break;
					}
					return schema_ptr;
				}

				static RecordSchema* GenerateItemSchema(){
					if (item_schema_ == NULL){
						item_schema_ = new RecordSchema(ITEM_TABLE_ID);
						std::vector<ColumnInfo*> columns;
						columns.push_back(new ColumnInfo("i_id", ValueType::INT));
						columns.push_back(new ColumnInfo("i_im_id", ValueType::INT));
						columns.push_back(new ColumnInfo("i_name", ValueType::VARCHAR, static_cast<size_t>(32)));
						columns.push_back(new ColumnInfo("i_price", ValueType::DOUBLE));
						columns.push_back(new ColumnInfo("i_data", ValueType::VARCHAR, static_cast<size_t>(64)));
						item_schema_->InsertColumns(columns);
						size_t column_ids[] = { 0 };
						item_schema_->SetPrimaryColumns(column_ids, 1);
						item_schema_->SetPartitionColumns(column_ids, 1);
					}
					return item_schema_;
				}

				static RecordSchema* GenerateWarehouseSchema(){
					if (warehouse_schema_ == NULL){
						warehouse_schema_ = new RecordSchema(WAREHOUSE_TABLE_ID);
						std::vector<ColumnInfo*> columns;
						columns.push_back(new ColumnInfo("w_id", ValueType::INT));
						columns.push_back(new ColumnInfo("w_name", ValueType::VARCHAR, static_cast<size_t>(16)));
						columns.push_back(new ColumnInfo("w_street_1", ValueType::VARCHAR, static_cast<size_t>(32)));
						columns.push_back(new ColumnInfo("w_street_2", ValueType::VARCHAR, static_cast<size_t>(32)));
						columns.push_back(new ColumnInfo("w_city", ValueType::VARCHAR, static_cast<size_t>(32)));
						columns.push_back(new ColumnInfo("w_state", ValueType::VARCHAR, static_cast<size_t>(2)));
						columns.push_back(new ColumnInfo("w_zip", ValueType::VARCHAR, static_cast<size_t>(9)));
						columns.push_back(new ColumnInfo("w_tax", ValueType::DOUBLE));
						columns.push_back(new ColumnInfo("w_ytd", ValueType::DOUBLE));
						warehouse_schema_->InsertColumns(columns);
						size_t column_ids[] = { 0 };
						warehouse_schema_->SetPrimaryColumns(column_ids, 1);
						warehouse_schema_->SetPartitionColumns(column_ids, 1);
					}
					return warehouse_schema_;
				}

				static RecordSchema* GenerateDistrictSchema(){
					if (district_schema_ == NULL){
						district_schema_ = new RecordSchema(DISTRICT_TABLE_ID);
						std::vector<ColumnInfo*> columns;
						columns.push_back(new ColumnInfo("d_id", ValueType::INT));
						columns.push_back(new ColumnInfo("d_w_id", ValueType::INT));
						columns.push_back(new ColumnInfo("d_name", ValueType::VARCHAR, static_cast<size_t>(16)));
						columns.push_back(new ColumnInfo("d_street_1", ValueType::VARCHAR, static_cast<size_t>(32)));
						columns.push_back(new ColumnInfo("d_street_2", ValueType::VARCHAR, static_cast<size_t>(32)));
						columns.push_back(new ColumnInfo("d_city", ValueType::VARCHAR, static_cast<size_t>(32)));
						columns.push_back(new ColumnInfo("d_state", ValueType::VARCHAR, static_cast<size_t>(2)));
						columns.push_back(new ColumnInfo("d_zip", ValueType::VARCHAR, static_cast<size_t>(9)));
						columns.push_back(new ColumnInfo("d_tax", ValueType::DOUBLE));
						columns.push_back(new ColumnInfo("d_ytd", ValueType::DOUBLE));
						columns.push_back(new ColumnInfo("d_next_o_id", ValueType::INT));
						district_schema_->InsertColumns(columns);
						size_t column_ids[] = { 0, 1 };
						district_schema_->SetPrimaryColumns(column_ids, 2);
						size_t partition_column_ids[] = { 1 };
						district_schema_->SetPartitionColumns(partition_column_ids, 1);
					}
					return district_schema_;
				}

				static RecordSchema* GenerateCustomerSchema(){
					if (customer_schema_ == NULL){
						customer_schema_ = new RecordSchema(CUSTOMER_TABLE_ID);
						std::vector<ColumnInfo*> columns;
						columns.push_back(new ColumnInfo("c_id", ValueType::INT));
						columns.push_back(new ColumnInfo("c_d_id", ValueType::INT));
						columns.push_back(new ColumnInfo("c_w_id", ValueType::INT));
						columns.push_back(new ColumnInfo("c_first", ValueType::VARCHAR, static_cast<size_t>(32)));
						columns.push_back(new ColumnInfo("c_middle", ValueType::VARCHAR, static_cast<size_t>(2)));
						columns.push_back(new ColumnInfo("c_last", ValueType::VARCHAR, static_cast<size_t>(32)));
						columns.push_back(new ColumnInfo("c_street_1", ValueType::VARCHAR, static_cast<size_t>(32)));
						columns.push_back(new ColumnInfo("c_street_2", ValueType::VARCHAR, static_cast<size_t>(32)));
						columns.push_back(new ColumnInfo("c_city", ValueType::VARCHAR, static_cast<size_t>(32)));
						columns.push_back(new ColumnInfo("c_state", ValueType::VARCHAR, static_cast<size_t>(2)));
						columns.push_back(new ColumnInfo("c_zip", ValueType::VARCHAR, static_cast<size_t>(9)));
						columns.push_back(new ColumnInfo("c_phone", ValueType::VARCHAR, static_cast<size_t>(32)));
						columns.push_back(new ColumnInfo("c_since", ValueType::INT64));
						columns.push_back(new ColumnInfo("c_credit", ValueType::VARCHAR, static_cast<size_t>(2)));
						columns.push_back(new ColumnInfo("c_credit_lim", ValueType::DOUBLE));
						columns.push_back(new ColumnInfo("c_discount", ValueType::DOUBLE));
						columns.push_back(new ColumnInfo("c_balance", ValueType::DOUBLE));
						columns.push_back(new ColumnInfo("c_ytd_payment", ValueType::DOUBLE));
						columns.push_back(new ColumnInfo("c_payment_cnt", ValueType::INT));
						columns.push_back(new ColumnInfo("c_delivery_cnt", ValueType::INT));
						columns.push_back(new ColumnInfo("c_data", ValueType::VARCHAR, static_cast<size_t>(500)));
						customer_schema_->InsertColumns(columns);
						size_t column_ids[] = { 0, 1, 2 };
						customer_schema_->SetPrimaryColumns(column_ids, 3);
						size_t partition_column_ids[] = { 2 };
						customer_schema_->SetPartitionColumns(partition_column_ids, 1);
					}
					return customer_schema_;
				}

				static RecordSchema* GenerateOrderSchema(){
					if (order_schema_ == NULL){
						order_schema_ = new RecordSchema(ORDER_TABLE_ID);
						std::vector<ColumnInfo*> columns;
						columns.push_back(new ColumnInfo("o_id", ValueType::INT));
						columns.push_back(new ColumnInfo("o_c_id", ValueType::INT));
						columns.push_back(new ColumnInfo("o_d_id", ValueType::INT));
						columns.push_back(new ColumnInfo("o_w_id", ValueType::INT));
						columns.push_back(new ColumnInfo("o_entry_d", ValueType::INT64));
						columns.push_back(new ColumnInfo("o_carrier_id", ValueType::INT));
						columns.push_back(new ColumnInfo("o_ol_cnt", ValueType::INT));
						columns.push_back(new ColumnInfo("o_all_local", ValueType::INT));
						order_schema_->InsertColumns(columns);
						size_t column_ids[] = { 0, 2, 3 };
						order_schema_->SetPrimaryColumns(column_ids, 3);
						//size_t o_column_ids[] = { 1, 2, 3 };
						//order_schema_->AddSecondaryColumns(o_column_ids, 3);
						// note that currently we do not run OrderStatus transaction.
						size_t partition_column_ids[] = { 3 };
						order_schema_->SetPartitionColumns(partition_column_ids, 1);
					}
					return order_schema_;
				}

				static RecordSchema* GenerateDistrictNewOrderSchema(){
					if (district_new_order_schema_ == NULL){
						district_new_order_schema_ = new RecordSchema(DISTRICT_NEW_ORDER_TABLE_ID);
						std::vector<ColumnInfo*> columns;
						columns.push_back(new ColumnInfo("d_id", ValueType::INT));
						columns.push_back(new ColumnInfo("w_id", ValueType::INT));
						columns.push_back(new ColumnInfo("o_id", ValueType::INT));
						district_new_order_schema_->InsertColumns(columns);
						size_t column_ids[] = { 0, 1 };
						district_new_order_schema_->SetPrimaryColumns(column_ids, 2);
						size_t partition_column_ids[] = { 1 };
						district_new_order_schema_->SetPartitionColumns(partition_column_ids, 1);
					}
					return district_new_order_schema_;
				}

				static RecordSchema* GenerateNewOrderSchema(){
					if (new_order_schema_ == NULL){
						new_order_schema_ = new RecordSchema(NEW_ORDER_TABLE_ID);
						std::vector<ColumnInfo*> columns;
						columns.push_back(new ColumnInfo("o_id", ValueType::INT));
						columns.push_back(new ColumnInfo("d_id", ValueType::INT));
						columns.push_back(new ColumnInfo("w_id", ValueType::INT));
						new_order_schema_->InsertColumns(columns);
						size_t column_ids[] = { 0, 1, 2 };
						new_order_schema_->SetPrimaryColumns(column_ids, 3);
						size_t partition_column_ids[] = { 2 };
						new_order_schema_->SetPartitionColumns(partition_column_ids, 1);
					}
					return new_order_schema_;
				}

				static RecordSchema* GenerateOrderLineSchema(){
					if (order_line_schema_ == NULL){
						order_line_schema_ = new RecordSchema(ORDER_LINE_TABLE_ID);
						std::vector<ColumnInfo*> columns;
						columns.push_back(new ColumnInfo("ol_o_id", ValueType::INT));
						columns.push_back(new ColumnInfo("ol_d_id", ValueType::INT));
						columns.push_back(new ColumnInfo("ol_w_id", ValueType::INT));
						columns.push_back(new ColumnInfo("ol_number", ValueType::INT));
						columns.push_back(new ColumnInfo("ol_i_id", ValueType::INT));
						columns.push_back(new ColumnInfo("ol_supply_w_id", ValueType::INT));
						columns.push_back(new ColumnInfo("ol_delivery_d", ValueType::INT64));
						columns.push_back(new ColumnInfo("ol_quantity", ValueType::INT));
						columns.push_back(new ColumnInfo("ol_amount", ValueType::DOUBLE));
						columns.push_back(new ColumnInfo("ol_dist_info", ValueType::VARCHAR, static_cast<size_t>(32)));
						order_line_schema_->InsertColumns(columns);
						size_t primary_column_ids[] = { 0, 1, 2, 3 };
						order_line_schema_->SetPrimaryColumns(primary_column_ids, 4);
						//size_t column_ids[] = { 0, 1, 2 };
						//order_line_schema_->AddSecondaryColumns(column_ids, 3);
						size_t partition_column_ids[] = { 0, 1, 2 };
						order_line_schema_->SetPartitionColumns(partition_column_ids, 3);
					}
					return order_line_schema_;
				}

				static RecordSchema* GenerateHistorySchema(){
					if (history_schema_ == NULL){
						history_schema_ = new RecordSchema(HISTORY_TABLE_ID);
						std::vector<ColumnInfo*> columns;
						columns.push_back(new ColumnInfo("h_c_id", ValueType::INT));
						columns.push_back(new ColumnInfo("h_c_d_id", ValueType::INT));
						columns.push_back(new ColumnInfo("h_c_w_id", ValueType::INT));
						columns.push_back(new ColumnInfo("h_d_id", ValueType::INT));
						columns.push_back(new ColumnInfo("h_w_id", ValueType::INT));
						columns.push_back(new ColumnInfo("h_date", ValueType::INT64));
						columns.push_back(new ColumnInfo("h_amount", ValueType::DOUBLE));
						columns.push_back(new ColumnInfo("h_data", ValueType::VARCHAR, static_cast<size_t>(32)));
						history_schema_->InsertColumns(columns);
						size_t primary_column_ids[] = { 3, 4, 5 };
						history_schema_->SetPrimaryColumns(primary_column_ids, 3);
						size_t column_ids[] = { 3, 4 };
						history_schema_->SetPartitionColumns(column_ids, 2);
					}
					return history_schema_;
				}

				static RecordSchema* GenerateStockSchema(){
					if (stock_schema_ == NULL){
						stock_schema_ = new RecordSchema(STOCK_TABLE_ID);
						std::vector<ColumnInfo*> columns;
						columns.push_back(new ColumnInfo("s_i_id", ValueType::INT));
						columns.push_back(new ColumnInfo("s_w_id", ValueType::INT));
						columns.push_back(new ColumnInfo("s_quantity", ValueType::INT));
						for (size_t i = 0; i < 10; ++i){
							columns.push_back(new ColumnInfo("s_dists" + std::to_string(i), ValueType::VARCHAR, static_cast<size_t>(32)));
						}
						columns.push_back(new ColumnInfo("s_ytd", ValueType::INT));
						columns.push_back(new ColumnInfo("s_order_cnt", ValueType::INT));
						columns.push_back(new ColumnInfo("s_remote_cnt", ValueType::INT));
						columns.push_back(new ColumnInfo("s_data", ValueType::VARCHAR, static_cast<size_t>(64)));
						stock_schema_->InsertColumns(columns);
						size_t column_ids[] = { 0, 1 };
						stock_schema_->SetPrimaryColumns(column_ids, 2);
						size_t partition_column_id[] = { 0 };
						stock_schema_->SetPartitionColumns(column_ids, 1);
					}
					return stock_schema_;
				}

			private:
				TpccSchema();
				TpccSchema(const TpccSchema&);
				TpccSchema & operator=(const TpccSchema&);

			private:
				static RecordSchema *item_schema_;
				static RecordSchema *warehouse_schema_;
				static RecordSchema *district_schema_;
				static RecordSchema *customer_schema_;
				static RecordSchema *order_schema_;
				static RecordSchema *new_order_schema_;
				static RecordSchema *order_line_schema_;
				static RecordSchema *history_schema_;
				static RecordSchema *stock_schema_;
				static RecordSchema *district_new_order_schema_;
			};
		}
	}
}

#endif
