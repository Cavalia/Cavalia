#include "TpccPopulator.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Tpcc{
			void TpccPopulator::StartPopulate(){
				StartPopulate(1, scale_params_->num_warehouses_);
			}
			
			void TpccPopulator::StartPopulate(const size_t &min_w_id, const size_t &max_w_id){
				// load items
				std::unordered_set<int> original_rows;
				TpccRandomGenerator::SelectUniqueIds(scale_params_->num_items_ / 10, 1, scale_params_->num_items_, original_rows);
				for (int item_id = 1; item_id <= scale_params_->num_items_; ++item_id){
					bool is_origin = (original_rows.find(item_id) != original_rows.end());
					// generate items
					ItemRecord *item_record = GenerateItemRecord(item_id, is_origin);
					InsertItemRecord(item_record);
					delete item_record;
					item_record = NULL;
				}
				// load warehouses
				for (int w_id = (int)min_w_id; w_id <= (int)max_w_id; ++w_id){
					// generate warehouses
					WarehouseRecord *warehouse_record = GenerateWarehouseRecord(w_id);
					InsertWarehouseRecord(warehouse_record);
					delete warehouse_record;
					warehouse_record = NULL;
					for (int d_id = 1; d_id <= scale_params_->num_districts_per_warehouse_; ++d_id){
						int d_next_o_id = scale_params_->num_customers_per_district_ + 1;
						// generate districts
						DistrictRecord *district_record = GenerateDistrictRecord(w_id, d_id, d_next_o_id);
						InsertDistrictRecord(district_record);
						delete district_record;
						district_record = NULL;

						std::unordered_set<int> selected_rows;
						TpccRandomGenerator::SelectUniqueIds(scale_params_->num_customers_per_district_ / 10, 1, scale_params_->num_customers_per_district_, selected_rows);

						for (int c_id = 1; c_id <= scale_params_->num_customers_per_district_; ++c_id){
							bool bad_credit = (selected_rows.find(c_id) != selected_rows.end());
							// generate customers
							CustomerRecord *customer_record = GenerateCustomerRecord(w_id, d_id, c_id, bad_credit);
							InsertCustomerRecord(customer_record);
							delete customer_record;
							customer_record = NULL;
							// generate histories
							HistoryRecord *history_record = GenerateHistoryRecord(w_id, d_id, c_id);
							InsertHistoryRecord(history_record);
							delete history_record;
							history_record = NULL;
						}

						// generate district new order
						// assume each customer has an order.
						// that is, num_customers_per_district == num_orders_per_district
						int initial_new_order_id = scale_params_->num_customers_per_district_ - scale_params_->num_new_orders_per_district_ + 1;
						DistrictNewOrderRecord *district_new_order_record = GenerateDistrictNewOrderRecord(w_id, d_id, initial_new_order_id);
						InsertDistrictNewOrderRecord(district_new_order_record);
						delete district_new_order_record;
						district_new_order_record = NULL;

						for (int o_id = 1; o_id <= scale_params_->num_customers_per_district_; ++o_id){
							int o_ol_cnt = TpccRandomGenerator::GenerateInteger(MIN_OL_CNT, MAX_OL_CNT);
							bool is_new_order = (o_id >= initial_new_order_id);
							// generate orders
							OrderRecord *order_record = GenerateOrderRecord(w_id, d_id, o_id, o_id, o_ol_cnt, is_new_order);
							InsertOrderRecord(order_record);
							delete order_record;
							order_record = NULL;

							// generate order lines
							for (int ol_number = 1; ol_number <= o_ol_cnt; ++ol_number){
								OrderLineRecord *order_line_record = GenerateOrderLineRecord(w_id, d_id, o_id, ol_number, scale_params_->num_items_, is_new_order);
								InsertOrderLineRecord(order_line_record);
								delete order_line_record;
								order_line_record = NULL;
							}

							if (is_new_order){
								// generate new orders
								NewOrderRecord *new_order_record = GenerateNewOrderRecord(w_id, d_id, o_id);
								InsertNewOrderRecord(new_order_record);
								delete new_order_record;
								new_order_record = NULL;
							}
						}
					}
					std::unordered_set<int> selected_rows;
					TpccRandomGenerator::SelectUniqueIds(scale_params_->num_items_ / 10, 1, scale_params_->num_items_, selected_rows);
					for (int i_id = 1; i_id <= scale_params_->num_items_; ++i_id){
						bool original = (selected_rows.find(i_id) != selected_rows.end());
						// generate stocks
						StockRecord *stock_record = GenerateStockRecord(w_id, i_id, original);
						InsertStockRecord(stock_record);
						delete stock_record;
						stock_record = NULL;
					}
				}
			}

			ItemRecord* TpccPopulator::GenerateItemRecord(const int &item_id, bool original) const{
				ItemRecord *record = new ItemRecord();
				record->i_id_ = item_id;
				record->i_im_id_ = TpccRandomGenerator::GenerateInteger(MIN_IM, MAX_IM);
				std::string name = TpccRandomGenerator::GenerateAString(MIN_I_NAME, MAX_I_NAME);
				memcpy(record->i_name_, name.c_str(), name.size());
				record->i_price_ = TpccRandomGenerator::GenerateFixedPoint(MONEY_DECIMALS, MIN_PRICE, MAX_PRICE);
				std::string data = TpccRandomGenerator::GenerateAString(MIN_I_DATA, MAX_I_DATA);
				memcpy(record->i_data_, data.c_str(), data.size());
				return record;
			}

			WarehouseRecord* TpccPopulator::GenerateWarehouseRecord(const int &w_id) const{
				WarehouseRecord *record = new WarehouseRecord();
				record->w_id_ = w_id;
				std::string name = TpccRandomGenerator::GenerateAString(MIN_NAME, MAX_NAME);
				memcpy(record->w_name_, name.c_str(), name.size());
				TpccRandomGenerator::GenerateAddress(record->w_street_1_, record->w_street_2_, record->w_city_, record->w_state_, record->w_zip_);
				record->w_tax_ = TpccRandomGenerator::GenerateTax();
				record->w_ytd_ = INITIAL_W_YTD;
				return record;
			}

			DistrictRecord* TpccPopulator::GenerateDistrictRecord(const int &d_w_id, const int &d_id, const int &d_next_o_id) const{
				DistrictRecord *record = new DistrictRecord();
				record->d_id_ = d_id;
				record->d_w_id_ = d_w_id;
				std::string name = TpccRandomGenerator::GenerateAString(MIN_NAME, MAX_NAME);
				memcpy(record->d_name_, name.c_str(), name.size());
				TpccRandomGenerator::GenerateAddress(record->d_street_1_, record->d_street_2_, record->d_city_, record->d_state_, record->d_zip_);
				record->d_tax_ = TpccRandomGenerator::GenerateTax();
				record->d_ytd_ = INITIAL_D_YTD;
				record->d_next_o_id_ = d_next_o_id;
				return record;
			}

			CustomerRecord* TpccPopulator::GenerateCustomerRecord(const int &c_w_id, const int &c_d_id, const int &c_id, bool bad_credit) const{
				CustomerRecord *record = new CustomerRecord();
				record->c_id_ = c_id;
				record->c_d_id_ = c_d_id;
				record->c_w_id_ = c_w_id;
				std::string first = TpccRandomGenerator::GenerateAString(MIN_FIRST, MAX_FIRST);
				memcpy(record->c_first_, first.c_str(), first.size());
				std::string middle = MIDDLE;
				memcpy(record->c_middle_, middle.c_str(), middle.size());
				std::string last;
				if (c_id < 1000){
					last = TpccRandomGenerator::GenerateLastName(c_id - 1);
				}
				else{
					last = TpccRandomGenerator::GenerateRandomLastName(CUSTOMERS_PER_DISTRICT);
				}
				memcpy(record->c_last_, last.c_str(), last.size());
				TpccRandomGenerator::GenerateAddress(record->c_street_1_, record->c_street_2_, record->c_city_, record->c_state_, record->c_zip_);
				std::string phone = TpccRandomGenerator::GenerateNString(PHONE, PHONE);
				memcpy(record->c_phone_, phone.c_str(), phone.size());
				record->c_since_ = TpccRandomGenerator::GenerateCurrentTime();
				std::string credit = GOOD_CREDIT;
				if (bad_credit){
					credit = BAD_CREDIT;
				}
				memcpy(record->c_credit_, credit.c_str(), credit.size());
				record->c_credit_lim_ = INITIAL_CREDIT_LIM;
				record->c_discount_ = TpccRandomGenerator::GenerateFixedPoint(DISCOUNT_DECIMALS, MIN_DISCOUNT, MAX_DISCOUNT);
				record->c_balance_ = INITIAL_BALANCE;
				record->c_ytd_payment_ = INITIAL_YTD_PAYMENT;
				record->c_payment_cnt_ = INITIAL_PAYMENT_CNT;
				record->c_delivery_cnt_ = INITIAL_DELIVERY_CNT;
				std::string data = TpccRandomGenerator::GenerateAString(MIN_C_DATA, MAX_C_DATA);
				memcpy(record->c_data_, data.c_str(), data.size());
				return record;
			}

			StockRecord* TpccPopulator::GenerateStockRecord(const int &s_w_id, const int &s_i_id, bool original) const{
				StockRecord *record = new StockRecord();
				record->s_i_id_ = s_i_id;
				record->s_w_id_ = s_w_id;
				record->s_quantity_ = TpccRandomGenerator::GenerateInteger(MIN_QUANTITY, MAX_QUANTITY);
				for (int i = 0; i < DISTRICTS_PER_WAREHOUSE; ++i){
					std::string dist = TpccRandomGenerator::GenerateAString(DIST, DIST);
					memcpy(record->s_dists_[i], dist.c_str(), dist.size());
				}
				record->s_ytd_ = 0;
				record->s_order_cnt_ = 0;
				record->s_remote_cnt_ = 0;
				std::string data = TpccRandomGenerator::GenerateAString(MIN_I_DATA, MAX_I_DATA);
				// TODO: FillOriginal() needs rewrite.
				//if (original){
				//	data = FillOriginal(data);
				//}
				memcpy(record->s_data_, data.c_str(), data.size());
				return record;
			}

			OrderRecord* TpccPopulator::GenerateOrderRecord(const int &o_w_id, const int &o_d_id, const int &o_id, const int &o_c_id, const int &o_ol_cnt, bool new_order) const{
				OrderRecord *record = new OrderRecord();
				record->o_id_ = o_id;
				record->o_c_id_ = o_c_id;
				record->o_d_id_ = o_d_id;
				record->o_w_id_ = o_w_id;
				record->o_entry_d_ = TpccRandomGenerator::GenerateCurrentTime();
				if (new_order){
					record->o_carrier_id_ = NULL_CARRIER_ID;
				}
				else{
					record->o_carrier_id_ = TpccRandomGenerator::GenerateInteger(MIN_CARRIER_ID, MAX_CARRIER_ID);
				}
				record->o_ol_cnt_ = o_ol_cnt;
				record->o_all_local_ = INITIAL_ALL_LOCAL;
				return record;
			}

			NewOrderRecord* TpccPopulator::GenerateNewOrderRecord(const int &w_id, const int &d_id, const int &o_id) const{
				NewOrderRecord *record = new NewOrderRecord();
				record->w_id_ = w_id;
				record->d_id_ = d_id;
				record->o_id_ = o_id;
				return record;
			}

			OrderLineRecord* TpccPopulator::GenerateOrderLineRecord(const int &ol_w_id, const int &ol_d_id, const int &ol_o_id, const int &ol_number, const int &max_items, bool new_order) const{
				OrderLineRecord *record = new OrderLineRecord();
				record->ol_o_id_ = ol_o_id;
				record->ol_d_id_ = ol_d_id;
				record->ol_w_id_ = ol_w_id;
				record->ol_number_ = ol_number;
				record->ol_i_id_ = TpccRandomGenerator::GenerateInteger(1, max_items);
				record->ol_supply_w_id_ = ol_w_id;
				record->ol_quantity_ = INITIAL_QUANTITY;
				if (new_order){
					record->ol_delivery_d_ = -1;
					record->ol_amount_ = TpccRandomGenerator::GenerateFixedPoint(MONEY_DECIMALS, MIN_AMOUNT, MAX_PRICE * MAX_OL_QUANTITY);
				}
				else{
					record->ol_delivery_d_ = TpccRandomGenerator::GenerateCurrentTime();
					record->ol_amount_ = 0.0;
				}
				std::string ol_dist_info = TpccRandomGenerator::GenerateAString(DIST, DIST);
				memcpy(record->ol_dist_info_, ol_dist_info.c_str(), ol_dist_info.size());
				return record;
			}

			HistoryRecord* TpccPopulator::GenerateHistoryRecord(const int &h_c_w_id, const int &h_c_d_id, const int &h_c_id) const{
				HistoryRecord *record = new HistoryRecord();
				record->h_c_id_ = h_c_id;
				record->h_c_d_id_ = h_c_d_id;
				record->h_c_w_id_ = h_c_w_id;
				record->h_d_id_ = h_c_d_id;
				record->h_w_id_ = h_c_w_id;
				record->h_date_ = TpccRandomGenerator::GenerateCurrentTime();
				record->h_amount_ = INITIAL_AMOUNT;
				std::string data = TpccRandomGenerator::GenerateAString(MIN_DATA, MAX_DATA);
				memcpy(record->h_data_, data.c_str(), data.size());
				return record;
			}

			DistrictNewOrderRecord* TpccPopulator::GenerateDistrictNewOrderRecord(const int &w_id, const int &d_id, const int &o_id) const{
				DistrictNewOrderRecord *record = new DistrictNewOrderRecord();
				record->w_id_ = w_id;
				record->d_id_ = d_id;
				record->o_id_ = o_id;
				return record;
			}

			void TpccPopulator::InsertItemRecord(const ItemRecord* record_ptr){
				char *data = new char[TpccSchema::GenerateItemSchema()->GetSchemaSize()];
				SchemaRecord *item_record = new SchemaRecord(TpccSchema::GenerateItemSchema(), data);
				item_record->SetColumn(0, reinterpret_cast<const char*>(&record_ptr->i_id_));
				item_record->SetColumn(1, reinterpret_cast<const char*>(&record_ptr->i_im_id_));
				item_record->SetColumn(2, record_ptr->i_name_, 32);
				item_record->SetColumn(3, reinterpret_cast<const char*>(&record_ptr->i_price_));
				item_record->SetColumn(4, record_ptr->i_data_, 64);
				storage_manager_->tables_[ITEM_TABLE_ID]->InsertRecord(new TableRecord(item_record));
			}

			void TpccPopulator::InsertWarehouseRecord(const WarehouseRecord* record_ptr){
				char *data = new char[TpccSchema::GenerateWarehouseSchema()->GetSchemaSize()];
				SchemaRecord *warehouse_record = new SchemaRecord(TpccSchema::GenerateWarehouseSchema(), data);
				warehouse_record->SetColumn(0, reinterpret_cast<const char*>(&record_ptr->w_id_));
				warehouse_record->SetColumn(1, record_ptr->w_name_, 16);
				warehouse_record->SetColumn(2, record_ptr->w_street_1_, 32);
				warehouse_record->SetColumn(3, record_ptr->w_street_2_, 32);
				warehouse_record->SetColumn(4, record_ptr->w_city_, 32);
				warehouse_record->SetColumn(5, record_ptr->w_state_, 2);
				warehouse_record->SetColumn(6, record_ptr->w_zip_, 9);
				warehouse_record->SetColumn(7, reinterpret_cast<const char*>(&record_ptr->w_tax_));
				warehouse_record->SetColumn(8, reinterpret_cast<const char*>(&record_ptr->w_ytd_));
				storage_manager_->tables_[WAREHOUSE_TABLE_ID]->InsertRecord(new TableRecord(warehouse_record));
			}

			void TpccPopulator::InsertDistrictRecord(const DistrictRecord* record_ptr){
				char *data = new char[TpccSchema::GenerateDistrictSchema()->GetSchemaSize()];
				SchemaRecord *district_record = new SchemaRecord(TpccSchema::GenerateDistrictSchema(), data);
				district_record->SetColumn(0, reinterpret_cast<const char*>(&record_ptr->d_id_));
				district_record->SetColumn(1, reinterpret_cast<const char*>(&record_ptr->d_w_id_));
				district_record->SetColumn(2, record_ptr->d_name_, 16);
				district_record->SetColumn(3, record_ptr->d_street_1_, 32);
				district_record->SetColumn(4, record_ptr->d_street_2_, 32);
				district_record->SetColumn(5, record_ptr->d_city_, 32);
				district_record->SetColumn(6, record_ptr->d_state_, 2);
				district_record->SetColumn(7, record_ptr->d_zip_, 9);
				district_record->SetColumn(8, reinterpret_cast<const char*>(&record_ptr->d_tax_));
				district_record->SetColumn(9, reinterpret_cast<const char*>(&record_ptr->d_ytd_));
				district_record->SetColumn(10, reinterpret_cast<const char*>(&record_ptr->d_next_o_id_));
				storage_manager_->tables_[DISTRICT_TABLE_ID]->InsertRecord(new TableRecord(district_record));
			}

			void TpccPopulator::InsertCustomerRecord(const CustomerRecord* record_ptr){
				char *data = new char[TpccSchema::GenerateCustomerSchema()->GetSchemaSize()];
				SchemaRecord *customer_record = new SchemaRecord(TpccSchema::GenerateCustomerSchema(), data);
				customer_record->SetColumn(0, reinterpret_cast<const char*>(&record_ptr->c_id_));
				customer_record->SetColumn(1, reinterpret_cast<const char*>(&record_ptr->c_d_id_));
				customer_record->SetColumn(2, reinterpret_cast<const char*>(&record_ptr->c_w_id_));
				customer_record->SetColumn(3, record_ptr->c_first_, 32);
				customer_record->SetColumn(4, record_ptr->c_middle_, 2);
				customer_record->SetColumn(5, record_ptr->c_last_, 32);
				customer_record->SetColumn(6, record_ptr->c_street_1_, 32);
				customer_record->SetColumn(7, record_ptr->c_street_2_, 32);
				customer_record->SetColumn(8, record_ptr->c_city_, 32);
				customer_record->SetColumn(9, record_ptr->c_state_, 2);
				customer_record->SetColumn(10, record_ptr->c_zip_, 9);
				customer_record->SetColumn(11, record_ptr->c_phone_, 32);
				customer_record->SetColumn(12, reinterpret_cast<const char*>(&record_ptr->c_since_));
				customer_record->SetColumn(13, record_ptr->c_credit_, 2);
				customer_record->SetColumn(14, reinterpret_cast<const char*>(&record_ptr->c_credit_lim_));
				customer_record->SetColumn(15, reinterpret_cast<const char*>(&record_ptr->c_discount_));
				customer_record->SetColumn(16, reinterpret_cast<const char*>(&record_ptr->c_balance_));
				customer_record->SetColumn(17, reinterpret_cast<const char*>(&record_ptr->c_ytd_payment_));
				customer_record->SetColumn(18, reinterpret_cast<const char*>(&record_ptr->c_payment_cnt_));
				customer_record->SetColumn(19, reinterpret_cast<const char*>(&record_ptr->c_delivery_cnt_));
				customer_record->SetColumn(20, record_ptr->c_data_, 500);
				storage_manager_->tables_[CUSTOMER_TABLE_ID]->InsertRecord(new TableRecord(customer_record));
			}

			void TpccPopulator::InsertStockRecord(const StockRecord* record_ptr){
				char *data = new char[TpccSchema::GenerateStockSchema()->GetSchemaSize()];
				SchemaRecord *stock_record = new SchemaRecord(TpccSchema::GenerateStockSchema(), data);
				stock_record->SetColumn(0, reinterpret_cast<const char*>(&record_ptr->s_i_id_));
				stock_record->SetColumn(1, reinterpret_cast<const char*>(&record_ptr->s_w_id_));
				stock_record->SetColumn(2, reinterpret_cast<const char*>(&record_ptr->s_quantity_));
				for (int i = 3; i < 3 + 10; ++i){
					stock_record->SetColumn(i, record_ptr->s_dists_[i - 3], 32);
				}
				stock_record->SetColumn(13, reinterpret_cast<const char*>(&record_ptr->s_ytd_));
				stock_record->SetColumn(14, reinterpret_cast<const char*>(&record_ptr->s_order_cnt_));
				stock_record->SetColumn(15, reinterpret_cast<const char*>(&record_ptr->s_remote_cnt_));
				stock_record->SetColumn(16, record_ptr->s_data_, 64);
				storage_manager_->tables_[STOCK_TABLE_ID]->InsertRecord(new TableRecord(stock_record));
			}

			void TpccPopulator::InsertOrderRecord(const OrderRecord* record_ptr){
				char *data = new char[TpccSchema::GenerateOrderSchema()->GetSchemaSize()];
				SchemaRecord *order_record = new SchemaRecord(TpccSchema::GenerateOrderSchema(), data);
				order_record->SetColumn(0, reinterpret_cast<const char*>(&record_ptr->o_id_));
				order_record->SetColumn(1, reinterpret_cast<const char*>(&record_ptr->o_c_id_));
				order_record->SetColumn(2, reinterpret_cast<const char*>(&record_ptr->o_d_id_));
				order_record->SetColumn(3, reinterpret_cast<const char*>(&record_ptr->o_w_id_));
				order_record->SetColumn(4, reinterpret_cast<const char*>(&record_ptr->o_entry_d_));
				order_record->SetColumn(5, reinterpret_cast<const char*>(&record_ptr->o_carrier_id_));
				order_record->SetColumn(6, reinterpret_cast<const char*>(&record_ptr->o_ol_cnt_));
				order_record->SetColumn(7, reinterpret_cast<const char*>(&record_ptr->o_all_local_));
				storage_manager_->tables_[ORDER_TABLE_ID]->InsertRecord(new TableRecord(order_record));
			}

			void TpccPopulator::InsertNewOrderRecord(const NewOrderRecord* record_ptr){
				char *data = new char[TpccSchema::GenerateNewOrderSchema()->GetSchemaSize()];
				SchemaRecord *new_order_record = new SchemaRecord(TpccSchema::GenerateNewOrderSchema(), data);
				new_order_record->SetColumn(0, reinterpret_cast<const char*>(&record_ptr->o_id_));
				new_order_record->SetColumn(1, reinterpret_cast<const char*>(&record_ptr->d_id_));
				new_order_record->SetColumn(2, reinterpret_cast<const char*>(&record_ptr->w_id_));
				storage_manager_->tables_[NEW_ORDER_TABLE_ID]->InsertRecord(new TableRecord(new_order_record));
			}

			void TpccPopulator::InsertOrderLineRecord(const OrderLineRecord* record_ptr){
				char *data = new char[TpccSchema::GenerateOrderLineSchema()->GetSchemaSize()];
				SchemaRecord *order_line_record = new SchemaRecord(TpccSchema::GenerateOrderLineSchema(), data);
				order_line_record->SetColumn(0, reinterpret_cast<const char*>(&record_ptr->ol_o_id_));
				order_line_record->SetColumn(1, reinterpret_cast<const char*>(&record_ptr->ol_d_id_));
				order_line_record->SetColumn(2, reinterpret_cast<const char*>(&record_ptr->ol_w_id_));
				order_line_record->SetColumn(3, reinterpret_cast<const char*>(&record_ptr->ol_number_));
				order_line_record->SetColumn(4, reinterpret_cast<const char*>(&record_ptr->ol_i_id_));
				order_line_record->SetColumn(5, reinterpret_cast<const char*>(&record_ptr->ol_supply_w_id_));
				order_line_record->SetColumn(6, reinterpret_cast<const char*>(&record_ptr->ol_delivery_d_));
				order_line_record->SetColumn(7, reinterpret_cast<const char*>(&record_ptr->ol_quantity_));
				order_line_record->SetColumn(8, reinterpret_cast<const char*>(&record_ptr->ol_amount_));
				order_line_record->SetColumn(9, record_ptr->ol_dist_info_, 32);
				storage_manager_->tables_[ORDER_LINE_TABLE_ID]->InsertRecord(new TableRecord(order_line_record));
			}

			void TpccPopulator::InsertHistoryRecord(const HistoryRecord* record_ptr){
				char *data = new char[TpccSchema::GenerateHistorySchema()->GetSchemaSize()];
				SchemaRecord *history_record = new SchemaRecord(TpccSchema::GenerateHistorySchema(), data);
				history_record->SetColumn(0, reinterpret_cast<const char*>(&record_ptr->h_c_id_));
				history_record->SetColumn(1, reinterpret_cast<const char*>(&record_ptr->h_c_d_id_));
				history_record->SetColumn(2, reinterpret_cast<const char*>(&record_ptr->h_c_w_id_));
				history_record->SetColumn(3, reinterpret_cast<const char*>(&record_ptr->h_d_id_));
				history_record->SetColumn(4, reinterpret_cast<const char*>(&record_ptr->h_w_id_));
				history_record->SetColumn(5, reinterpret_cast<const char*>(&record_ptr->h_date_));
				history_record->SetColumn(6, reinterpret_cast<const char*>(&record_ptr->h_amount_));
				history_record->SetColumn(7, record_ptr->h_data_, 32);
				storage_manager_->tables_[HISTORY_TABLE_ID]->InsertRecord(new TableRecord(history_record));
			}

			void TpccPopulator::InsertDistrictNewOrderRecord(const DistrictNewOrderRecord* record_ptr){
				char *data = new char[TpccSchema::GenerateDistrictNewOrderSchema()->GetSchemaSize()];
				SchemaRecord *district_new_order_record = new SchemaRecord(TpccSchema::GenerateDistrictNewOrderSchema(), data);
				district_new_order_record->SetColumn(0, reinterpret_cast<const char*>(&record_ptr->d_id_));
				district_new_order_record->SetColumn(1, reinterpret_cast<const char*>(&record_ptr->w_id_));
				district_new_order_record->SetColumn(2, reinterpret_cast<const char*>(&record_ptr->o_id_));
				storage_manager_->tables_[DISTRICT_NEW_ORDER_TABLE_ID]->InsertRecord(new TableRecord(district_new_order_record));
			}

		}
	}
}
