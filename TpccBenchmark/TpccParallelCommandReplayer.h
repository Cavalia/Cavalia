#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_TPCC_SLICE_REPLAYER_H__
#define __CAVALIA_TPCC_BENCHMARK_TPCC_SLICE_REPLAYER_H__

#include <Replayer/ParallelCommandReplayer.h>

#include "SliceProcedures/StockSlice.h"
#include "SliceProcedures/ItemSlice.h"
#include "SliceProcedures/HistorySlice.h"
#include "SliceProcedures/WarehouseSlice.h"
#include "SliceProcedures/District9Slice.h"
#include "SliceProcedures/CustomerSlice.h"
#include "SliceProcedures/District10Slice.h"
#include "SliceProcedures/NewOrderSlice.h"
#include "SliceProcedures/OrderLineSlice.h"
#include "SliceProcedures/OrderSlice.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Tpcc{
			namespace Replayer{
				using namespace Cavalia::Database;
				using namespace Cavalia::Benchmark::Tpcc::ReplaySlices;
				class TpccParallelCommandReplayer : public ParallelCommandReplayer{
				public:
					TpccParallelCommandReplayer(const std::string &filename, BaseStorageManager *const storage_manager, const size_t &thread_count) : ParallelCommandReplayer(filename, storage_manager, thread_count){}
					virtual ~TpccParallelCommandReplayer(){}

				private:
					virtual TxnParam* DeserializeParam(const size_t &param_type, const CharArray &entry){
						TxnParam *tuple;
						if (param_type == TupleType::DELIVERY){
							tuple = new DeliveryParam();
						}
						else if (param_type == TupleType::NEW_ORDER){
							tuple = new NewOrderParam();
						}
						else if (param_type == TupleType::PAYMENT){
							tuple = new PaymentParam();
						}
						else{
							return NULL;
						}
						tuple->Deserialize(entry);
						return tuple;
					}

					virtual void BuildGraph() {
						schedule_graph_.SetNodeCount(kSliceCount);
						for (size_t i = 0; i < kSliceCount; ++i){
							schedule_graph_.SetPartition(i, slice_counts_[i]);
						}

						schedule_graph_.AddDependence(S_SCAN, WAREHOUSE_SLICE);
						schedule_graph_.AddDependence(S_SCAN, DISTRICT_9_SLICE);
						schedule_graph_.AddDependence(S_SCAN, DISTRICT_10_SLICE);
						schedule_graph_.AddDependence(S_SCAN, CUSTOMER_SLICE);
						schedule_graph_.AddDependence(S_SCAN, HISTORY_SLICE);
						schedule_graph_.AddDependence(S_SCAN, ITEM_SLICE);
						schedule_graph_.AddDependence(S_SCAN, STOCK_SLICE);

						//schedule_graph_.AddDependence(WAREHOUSE_SLICE, CUSTOMER_SLICE);
						//schedule_graph_.AddDependence(DISTRICT_10_SLICE, CUSTOMER_SLICE);
						//schedule_graph_.AddDependence(ITEM_SLICE, CUSTOMER_SLICE);
						// redundant
						//schedule_graph_.AddDependence(DISTRICT_10_SLICE, ORDER_SLICE);
						schedule_graph_.AddDependence(DISTRICT_10_SLICE, NEW_ORDER_SLICE);
						// redundant
						//schedule_graph_.AddDependence(DISTRICT_10_SLICE, ORDER_LINE_SLICE);
						schedule_graph_.AddDependence(STOCK_SLICE, ORDER_LINE_SLICE);
						schedule_graph_.AddDependence(ITEM_SLICE, ORDER_LINE_SLICE);
						schedule_graph_.AddDependence(NEW_ORDER_SLICE, S_SHUFFLE);
						schedule_graph_.AddDependence(S_SHUFFLE, ORDER_SLICE);
						schedule_graph_.AddDependence(S_SHUFFLE, ORDER_LINE_SLICE);
						// redundant
						//schedule_graph_.AddDependence(NEW_ORDER_SLICE, CUSTOMER_SLICE);
						schedule_graph_.AddDependence(ORDER_SLICE, CUSTOMER_SLICE);
						schedule_graph_.AddDependence(ORDER_LINE_SLICE, CUSTOMER_SLICE);
						//schedule_graph_.AddDependence(S_WAREHOUSE_8, DISTRICT_9_SLICE);
						scheduler_.RegisterGraph(&schedule_graph_, layer_count_);
					}

					virtual void PrepareParams() {
						std::cout << "layer_count=" << layer_count_ << std::endl;
						new_order_query_params_ = new ParamBatch[layer_count_];
						payment_query_params_ = new ParamBatch[layer_count_];
						customer_params_ = new ParamBatch*[layer_count_];
						for (size_t i = 0; i < slice_counts_[CUSTOMER_SLICE]; ++i){
							customer_params_[i] = new ParamBatch[layer_count_];
						}

						order_params_ = new ParamBatch*[slice_counts_[ORDER_SLICE]];
						for (size_t i = 0; i < slice_counts_[ORDER_SLICE]; ++i) {
							order_params_[i] = new ParamBatch[layer_count_];
						}

						order_line_params_ = new ParamBatchWrapper*[slice_counts_[ORDER_LINE_SLICE]];
						for (size_t i = 0; i < slice_counts_[ORDER_LINE_SLICE]; ++i) {
							order_line_params_[i] = new ParamBatchWrapper[layer_count_];
						}

						item_params_ = new ParamBatchWrapper*[slice_counts_[ITEM_SLICE]];
						for (size_t i = 0; i < slice_counts_[ITEM_SLICE]; ++i) {
							item_params_[i] = new ParamBatchWrapper[layer_count_];
						}

						//stock_params_ = new ParamBatchWrapper*[slice_counts_[STOCK_SLICE]];
						//for (size_t i = 0; i < slice_counts_[STOCK_SLICE]; ++i) {
						//	stock_params_[i] = new ParamBatchWrapper[layer_count_];
						//}

						/*for (size_t i = 0; i < layer_count_; ++i){
						Scan(i);
						}*/
					}

					void RunWorker(const size_t &core_id){
						// note that core_id is not equal to thread_id.
						PinToCore(core_id);
#if defined(NUMA)
						size_t node_id = numa_node_of_cpu(core_id);
#else
						size_t node_id = 0;
#endif
						ALLOCATE_SLICE(WarehouseSlice, warehouse_slice);
						ALLOCATE_SLICE(StockSlice, stock_slice);
						ALLOCATE_SLICE(ItemSlice, item_slice);
						ALLOCATE_SLICE(HistorySlice, history_slice);
						ALLOCATE_SLICE(District9Slice, district_9_slice);
						ALLOCATE_SLICE(District10Slice, district_10_slice);
						ALLOCATE_SLICE(CustomerSlice, customer_slice);
						ALLOCATE_SLICE(NewOrderSlice, new_order_slice);
						ALLOCATE_SLICE(OrderLineSlice, order_line_slice);
						ALLOCATE_SLICE(OrderSlice, order_slice);

						start_timestamp_ = TimeMeasurer::GetTimePoint();
						BEGIN_CORE_EXECUTE_TIME_MEASURE(core_id);
						while (1){
							BEGIN_CORE_WAIT_TIME_MEASURE(core_id);
							TaskID task_id = scheduler_.AcquireTask();
							END_CORE_WAIT_TIME_MEASURE(core_id, task_id.slice_id_);
							if (task_id.slice_id_ == SIZE_MAX){
								END_CORE_EXECUTE_TIME_MEASURE(core_id);
								end_timestamp_ = TimeMeasurer::GetTimePoint();
								return;
							}
							BEGIN_CORE_BUSY_TIME_MEASURE(core_id);
							switch (task_id.slice_id_){
							case S_SCAN:
								RunScan(task_id);
								break;
							case WAREHOUSE_SLICE:
								RunWarehouseTask(warehouse_slice, task_id);
								break;
							case DISTRICT_9_SLICE:
								RunDistrict9Task(district_9_slice, task_id);
								break;
							case DISTRICT_10_SLICE:
								RunDistrict10Task(district_10_slice, task_id);
								break;
							case HISTORY_SLICE:
								RunHistoryTask(history_slice, task_id);
								break;
							case CUSTOMER_SLICE:
								RunCustomerTask(customer_slice, task_id);
								break;
							case NEW_ORDER_SLICE:
								RunNewOrderTask(new_order_slice, task_id);
								break;
							case ORDER_LINE_SLICE:
								RunOrderLineTask(order_line_slice, task_id);
								break;
							case ORDER_SLICE:
								RunOrderTask(order_slice, task_id);
								break;
							case STOCK_SLICE:
								RunStockTask(stock_slice, task_id);
								break;
							case ITEM_SLICE:
								RunItemTask(item_slice, task_id);
								break;
							case S_SHUFFLE:
								RunShuffle(task_id);
								break;
							default:
								assert(false);
								break;
							}
							END_CORE_BUSY_TIME_MEASURE(core_id, task_id.slice_id_);
						}
					}

					void Scan(const size_t &layer_id){
						for (size_t idx = 0; idx < execution_batches_[layer_id]->size(); ++idx) {
							EventTuple *tuple = execution_batches_[layer_id]->get(idx);
							if (tuple->type_ == TupleType::NEW_ORDER) {
								NewOrderParam* new_order_param = static_cast<NewOrderParam*>(tuple);
								for (size_t i = 0; i < new_order_param->ol_cnt_; ++i) {
									// partition according to item_id.
									HashcodeType hashcode = FastHash((char*)(&new_order_param->i_ids_[i]));
									size_t item_part_id = hashcode % slice_counts_[ITEM_SLICE];
									item_params_[item_part_id][layer_id].push_back(tuple, i);
								}
								size_t o_part_id = new_order_param->w_id_ % slice_counts_[ORDER_SLICE];
								order_params_[o_part_id][layer_id].push_back(tuple);
								new_order_query_params_[layer_id].push_back(tuple);
							}
							else if (tuple->type_ == TupleType::PAYMENT) {
								PaymentParam* payment_param = static_cast<PaymentParam*>(tuple);
								size_t customer_part_id = payment_param->w_id_ % slice_counts_[CUSTOMER_SLICE];
								customer_params_[customer_part_id][layer_id].push_back(tuple);
								payment_query_params_[layer_id].push_back(tuple);
							}
							else if (tuple->type_ == TupleType::DELIVERY) {
								DeliveryParam* delivery_param = static_cast<DeliveryParam*>(tuple);
								size_t o_part_id = delivery_param->w_id_ % slice_counts_[ORDER_SLICE];
								order_params_[o_part_id][layer_id].push_back(tuple);
								size_t customer_part_id = delivery_param->w_id_ % slice_counts_[CUSTOMER_SLICE];
								customer_params_[customer_part_id][layer_id].push_back(tuple);
							}
						}
					}

					void RunScan(const TaskID &task_id){
						size_t layer_id = task_id.layer_id_;
						for (size_t idx = 0; idx < execution_batches_[layer_id]->size(); ++idx) {
							EventTuple *tuple = execution_batches_[layer_id]->get(idx);
							if (tuple->type_ == TupleType::NEW_ORDER) {
								NewOrderParam* new_order_param = static_cast<NewOrderParam*>(tuple);
								for (size_t i = 0; i < new_order_param->ol_cnt_; ++i) {
									// partition according to item_id.
									//HashcodeType hashcode = FastHash((char*)(&new_order_param->i_ids_[i]));
									size_t item_part_id = new_order_param->i_ids_[i] % slice_counts_[ITEM_SLICE];
									item_params_[item_part_id][layer_id].push_back(tuple, i);
								}
								size_t o_part_id = new_order_param->w_id_ % slice_counts_[ORDER_SLICE];
								order_params_[o_part_id][layer_id].push_back(tuple);
								new_order_query_params_[layer_id].push_back(tuple);
							}

							else if (tuple->type_ == TupleType::PAYMENT) {
								PaymentParam* payment_param = static_cast<PaymentParam*>(tuple);
								size_t customer_part_id = payment_param->w_id_ % slice_counts_[CUSTOMER_SLICE];
								customer_params_[customer_part_id][layer_id].push_back(tuple);
								payment_query_params_[layer_id].push_back(tuple);
							}
							else if (tuple->type_ == TupleType::DELIVERY) {
								DeliveryParam* delivery_param = static_cast<DeliveryParam*>(tuple);
								size_t o_part_id = delivery_param->w_id_ % slice_counts_[ORDER_SLICE];
								order_params_[o_part_id][layer_id].push_back(tuple);
								size_t customer_part_id = delivery_param->w_id_ % slice_counts_[CUSTOMER_SLICE];
								customer_params_[customer_part_id][layer_id].push_back(tuple);
							}
						}
						scheduler_.CompleteTask(task_id);
					}

					void RunWarehouseTask(WarehouseSlice *warehouse_slice, const TaskID task_id){
						warehouse_slice->Execute(&new_order_query_params_[task_id.layer_id_]);
						scheduler_.CompleteTask(task_id);
					}

					void RunWarehouse8Task(Warehouse8Slice *warehouse_8_slice, const TaskID task_id){
						warehouse_8_slice->Execute(&payment_query_params_[task_id.layer_id_]);
						scheduler_.CompleteTask(task_id);
					}

					void RunDistrict9Task(District9Slice *district_9_slice, const TaskID task_id){
						district_9_slice->Execute(&payment_query_params_[task_id.layer_id_]);
						scheduler_.CompleteTask(task_id);
					}

					void RunDistrict10Task(District10Slice *district_10_slice, const TaskID task_id){
						district_10_slice->Execute(&new_order_query_params_[task_id.layer_id_]);
						scheduler_.CompleteTask(task_id);
					}

					void RunHistoryTask(HistorySlice *history_slice, const TaskID task_id){
						history_slice->Execute(&payment_query_params_[task_id.layer_id_]);
						scheduler_.CompleteTask(task_id);
					}

					void RunStockTask(StockSlice *stock_slice, const TaskID task_id){
						stock_slice->Execute(&item_params_[task_id.partition_id_][task_id.layer_id_]);
						scheduler_.CompleteTask(task_id);
					}

					void RunItemTask(ItemSlice *item_slice, const TaskID task_id){
						item_slice->Execute(&item_params_[task_id.partition_id_][task_id.layer_id_]);
						scheduler_.CompleteTask(task_id);
					}

					void RunNewOrderTask(NewOrderSlice *new_order_slice, const TaskID task_id){
						new_order_slice->Execute(&order_params_[task_id.partition_id_][task_id.layer_id_]);
						scheduler_.CompleteTask(task_id);
					}

					void RunShuffle(const TaskID task_id){
						size_t layer_id = task_id.layer_id_;
						for (size_t idx = 0; idx < execution_batches_[layer_id]->size(); ++idx) {
							//for (size_t idx = 0; idx < delivery_new_order_query_params_[layer_id].size(); ++idx) {
							EventTuple *tuple = execution_batches_[layer_id]->get(idx);
							if (tuple->type_ == TupleType::NEW_ORDER){
								NewOrderParam *new_order_param = static_cast<NewOrderParam*>(tuple);
								HashcodeType hashcode = FastHash((char*)(&new_order_param->next_o_id_), (char*)(&new_order_param->d_id_), (char*)(&new_order_param->w_id_));
								//size_t o_part_id = hashcode % slice_counts_[ORDER_SLICE];
								//order_params_[o_part_id][layer_id].push_back(tuple, 0);
								size_t ol_part_id = hashcode % slice_counts_[ORDER_LINE_SLICE];
								order_line_params_[ol_part_id][layer_id].push_back(tuple, 0);
							}
							else if (tuple->type_ == TupleType::DELIVERY){
								DeliveryParam *delivery_param = static_cast<DeliveryParam*>(tuple);
								for (size_t i = 0; i < DISTRICTS_PER_WAREHOUSE; ++i){
									int d_id = i + 1;
									HashcodeType hashcode = FastHash((char*)(&delivery_param->no_o_ids_[i]), (char*)(&d_id), (char*)(&delivery_param->w_id_));
									//size_t o_part_id = hashcode % slice_counts_[ORDER_SLICE];
									//order_params_[o_part_id][layer_id].push_back(tuple, i);
									size_t ol_part_id = hashcode % slice_counts_[ORDER_LINE_SLICE];
									order_line_params_[ol_part_id][layer_id].push_back(tuple, i);
								}
							}
						}
						scheduler_.CompleteTask(task_id);
					}

					void RunOrderTask(OrderSlice *order_slice, const TaskID task_id){
						order_slice->Execute(&order_params_[task_id.partition_id_][task_id.layer_id_]);
						scheduler_.CompleteTask(task_id);
					}

					void RunOrderLineTask(OrderLineSlice *order_line_slice, const TaskID task_id){
						order_line_slice->Execute(&order_line_params_[task_id.partition_id_][task_id.layer_id_]);
						scheduler_.CompleteTask(task_id);
					}

					void RunCustomer16Task(CustomerSlice *customer_16_slice, const TaskID task_id){
						customer_16_slice->Execute(&customer_params_[task_id.partition_id_][task_id.layer_id_]);
						scheduler_.CompleteTask(task_id);
					}


				private:
					TpccParallelCommandReplayer(const TpccParallelCommandReplayer &);
					TpccParallelCommandReplayer& operator=(const TpccParallelCommandReplayer &);

				protected:
					ParamBatch *new_order_query_params_;
					ParamBatch *payment_query_params_;

					// consumed by customer_16.
					ParamBatch **customer_params_;

					//// consumed by new_order
					//ParamBatch **new_order_params_;

					// partitioned by item_id.
					// consumed by item.
					ParamBatchWrapper **item_params_;

					// consumed by stock.
					//ParamBatchWrapper **stock_params_;

					// consumed by order.
					ParamBatch **order_params_;

					// partitioned by order_id, district_id, and warehouse_id.
					// consumed by order_line.
					ParamBatchWrapper **order_line_params_;
				};
			}
		}
	}
}

#endif
