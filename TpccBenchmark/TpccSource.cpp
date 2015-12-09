#include "TpccSource.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Tpcc{
			void TpccSource::StartGeneration(){
				double frequency_weights[5];
				frequency_weights[0] = FREQUENCY_DELIVERY;
				frequency_weights[1] = FREQUENCY_PAYMENT;
				frequency_weights[2] = FREQUENCY_NEW_ORDER;
				frequency_weights[3] = FREQUENCY_ORDER_STATUS;
				frequency_weights[4] = FREQUENCY_STOCK_LEVEL;

				double total = 0;
				for (size_t i = 0; i < 5; ++i){
					total += frequency_weights[i];
				}
				for (size_t i = 0; i < 5; ++i){
					frequency_weights[i] = frequency_weights[i] * 1.0 / total * 100;
				}
				for (size_t i = 1; i < 5; ++i){
					frequency_weights[i] += frequency_weights[i - 1];
				}

				if (source_type_ == RANDOM_SOURCE){
					ParamBatch *tuples = new ParamBatch(gParamBatchSize);
					for (size_t i = 0; i < num_transactions_; ++i){
						int x = TpccRandomGenerator::GenerateInteger(1, 100);
						if (x <= frequency_weights[0]) {
							DeliveryParam *param = NULL;
							param = GenerateDeliveryParam();
							tuples->push_back(param);
						}
						else if (x <= frequency_weights[1]) {
							PaymentParam *param = NULL;
							param = GeneratePaymentParam();
							tuples->push_back(param);
						}
						else if (x <= frequency_weights[2]) {
							NewOrderParam *param = NULL;
							param = GenerateNewOrderParam();
							tuples->push_back(param);
						}
						else if (x <= frequency_weights[3]) {
							OrderStatusParam *param = NULL;
							param = GenerateOrderStatusParam();
							tuples->push_back(param);
						}
						else{
							StockLevelParam *param = NULL;
							param = GenerateStockLevelParam();
							tuples->push_back(param);
						}
						if ((i + 1) % gParamBatchSize == 0){
							DumpToDisk(tuples);
							redirector_ptr_->PushParameterBatch(tuples);
							tuples = new ParamBatch(gParamBatchSize);
						}
					}
					if (tuples->size() != 0){
						DumpToDisk(tuples);
						redirector_ptr_->PushParameterBatch(tuples);
					}
					else{
						delete tuples;
						tuples = NULL;
					}
				}
				else if (source_type_ == PARTITION_SOURCE || source_type_ == SELECT_SOURCE){
					size_t partition_id = 0;
					if (source_type_ == SELECT_SOURCE){
						partition_id = partition_id_;
					}
					ParamBatch *tuples = new ParamBatch(gParamBatchSize);
					for (size_t i = 0; i < num_transactions_; ++i){
						// need to randomly generate one.
						size_t warehouse_id = 0;
						while (1){
							warehouse_id = TpccRandomGenerator::GenerateWarehouseId(scale_params_->starting_warehouse_, scale_params_->ending_warehouse_);
							if ((warehouse_id - 1) % partition_count_ == partition_id){
								break;
							}
						}
						int x = TpccRandomGenerator::GenerateInteger(1, 100);
						if (x <= frequency_weights[0]) {
							DeliveryParam *param = NULL;
							param = GenerateDeliveryParam(warehouse_id);
							tuples->push_back(param);
						}
						else if (x <= frequency_weights[1]) {
							PaymentParam *param = NULL;
							param = GeneratePaymentParam(warehouse_id);
							tuples->push_back(param);
						}
						else if (x <= frequency_weights[2]) {
							NewOrderParam *param = NULL;
							param = GenerateNewOrderParam(warehouse_id);
							tuples->push_back(param);
						}
						else if (x <= frequency_weights[3]) {
							OrderStatusParam *param = NULL;
							param = GenerateOrderStatusParam(warehouse_id);
							tuples->push_back(param);
						}
						else{
							StockLevelParam *param = NULL;
							param = GenerateStockLevelParam(warehouse_id);
							tuples->push_back(param);
						}
						if ((i + 1) % gParamBatchSize == 0){
							DumpToDisk(tuples);
							redirector_ptr_->PushParameterBatch(tuples);
							tuples = new ParamBatch(gParamBatchSize);
							if (source_type_ == PARTITION_SOURCE){
								partition_id = (partition_id + 1) % partition_count_;
							}
						}
					}
					if (tuples->size() != 0){
						DumpToDisk(tuples);
						redirector_ptr_->PushParameterBatch(tuples);
					}
					else{
						delete tuples;
						tuples = NULL;
					}
				}
			}

			DeliveryParam* TpccSource::GenerateDeliveryParam(const int &w_id) const{
				DeliveryParam *param = new DeliveryParam();
				if (w_id == -1){
					param->w_id_ = TpccRandomGenerator::GenerateWarehouseId(scale_params_->starting_warehouse_, scale_params_->ending_warehouse_);
				}
				else{
					param->w_id_ = w_id;
				}
				param->o_carrier_id_ = TpccRandomGenerator::GenerateInteger(MIN_CARRIER_ID, MAX_CARRIER_ID);
				param->ol_delivery_d_ = TpccRandomGenerator::GenerateCurrentTime();
				return param;
			}

			NewOrderParam* TpccSource::GenerateNewOrderParam(const int &w_id) const{
				NewOrderParam *param = new NewOrderParam();
				if (w_id == -1){
					param->w_id_ = TpccRandomGenerator::GenerateWarehouseId(scale_params_->starting_warehouse_, scale_params_->ending_warehouse_);
				}
				else{
					param->w_id_ = w_id;
				}
				param->d_id_ = TpccRandomGenerator::GenerateDistrictId(scale_params_->num_districts_per_warehouse_);
				param->c_id_ = TpccRandomGenerator::GenerateCustomerId(scale_params_->num_customers_per_district_);
				param->ol_cnt_ = static_cast<size_t>(TpccRandomGenerator::GenerateInteger(MIN_OL_CNT, MAX_OL_CNT));
				param->o_entry_d_ = TpccRandomGenerator::GenerateCurrentTime();
				// generate abort here!
				//bool rollback = (TpccRandomGenerator::GenerateInteger(1, 100) == 1);
				bool rollback = false;
				std::unordered_set<int> exist_items;
				for (size_t i = 0; i < param->ol_cnt_; ++i){
					if (rollback && i == param->ol_cnt_ - 1){
						param->i_ids_[i] = scale_params_->num_items_ + 1;
					}
					else{
						while (true){
							int item_id = TpccRandomGenerator::GenerateItemId(scale_params_->num_items_);
							// guarantee the uniqueness of the item.
							if (exist_items.find(item_id) == exist_items.end()){
								param->i_ids_[i] = item_id;
								exist_items.insert(item_id);
								break;
							}
						}
					}
					bool remote = (TpccRandomGenerator::GenerateInteger(1, 100) <= (int)dist_ratio_);
					if (scale_params_->num_warehouses_ > 1 && remote){
						param->i_w_ids_[i] = TpccRandomGenerator::GenerateIntegerExcluding(scale_params_->starting_warehouse_, scale_params_->ending_warehouse_, param->w_id_);
					}
					else{
						param->i_w_ids_[i] = param->w_id_;
					}
					param->i_qtys_[i] = TpccRandomGenerator::GenerateInteger(1, MAX_OL_QUANTITY);
				}
				return param;
			}

			PaymentParam* TpccSource::GeneratePaymentParam(const int &w_id) const{
				PaymentParam *param = new PaymentParam();
				// Return parameters for PAYMENT
				int x = TpccRandomGenerator::GenerateInteger(1, 100);
				int y = TpccRandomGenerator::GenerateInteger(1, 100);
				if (w_id == -1){
					param->w_id_ = TpccRandomGenerator::GenerateWarehouseId(scale_params_->starting_warehouse_, scale_params_->ending_warehouse_);
				}
				else{
					param->w_id_ = w_id;
				}
				param->d_id_ = TpccRandomGenerator::GenerateDistrictId(scale_params_->num_districts_per_warehouse_);
				param->c_w_id_ = -1;
				param->c_d_id_ = -1;
				//param->c_id_ = -1;
				//param->c_last_ = "";
				param->h_amount_ = TpccRandomGenerator::GenerateFixedPoint(2, MIN_PAYMENT, MAX_PAYMENT);
				param->h_date_ = TpccRandomGenerator::GenerateCurrentTime();

				// 85%: paying through own warehouse (or there is only 1 warehouse)
				if (scale_params_->num_warehouses_ == 1 || x <= 85) {
					param->c_w_id_ = param->w_id_;
					param->c_d_id_ = param->d_id_;
				}
				// 15%: paying through another warehouse
				else {
					// select in range [1, num_warehouses] excluding w_id
					param->c_w_id_ = TpccRandomGenerator::GenerateIntegerExcluding(scale_params_->starting_warehouse_, scale_params_->ending_warehouse_, param->w_id_);
					param->c_d_id_ = TpccRandomGenerator::GenerateDistrictId(scale_params_->num_districts_per_warehouse_);
				}
				// 60%: payment by last name
				//if (y <= 60) {
					//param->c_last_ = TpccRandomGenerator::GenerateRandomLastName(scale_params_->num_customers_per_district_);
				//}
				// 40%: payment by id
				//else {
				param->c_id_ = TpccRandomGenerator::GenerateCustomerId(scale_params_->num_customers_per_district_);
				//}
				return param;
			}

			OrderStatusParam* TpccSource::GenerateOrderStatusParam(const int &w_id) const{
				OrderStatusParam *param = new OrderStatusParam();
				if (w_id == -1){
					param->w_id_ = TpccRandomGenerator::GenerateWarehouseId(scale_params_->starting_warehouse_, scale_params_->ending_warehouse_);
				}
				else{
					param->w_id_ = w_id;
				}
				param->d_id_ = TpccRandomGenerator::GenerateDistrictId(scale_params_->num_districts_per_warehouse_);
				//param->c_last_ = "";
				// check order status by id
				param->c_id_ = TpccRandomGenerator::GenerateCustomerId(scale_params_->num_customers_per_district_);
				return param;
			}

			StockLevelParam* TpccSource::GenerateStockLevelParam(const int &w_id) const{
				StockLevelParam *param = new StockLevelParam();
				if (w_id == -1){
					param->w_id_ = TpccRandomGenerator::GenerateWarehouseId(scale_params_->starting_warehouse_, scale_params_->ending_warehouse_);
				}
				else{
					param->w_id_ = w_id;
				}
				param->d_id_ = TpccRandomGenerator::GenerateDistrictId(scale_params_->num_districts_per_warehouse_);
				return param;
			}
		}
	}
}
