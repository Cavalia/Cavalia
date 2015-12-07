#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_TPCC_HSTORE_EXECUTOR_H__
#define __CAVALIA_TPCC_BENCHMARK_TPCC_HSTORE_EXECUTOR_H__

#include <Executor/HStoreExecutor.h>

#include "ShardProcedures/DeliveryShardProcedure.h"
#include "ShardProcedures/NewOrderShardProcedure.h"
#include "ShardProcedures/PaymentShardProcedure.h"
#include "ShardProcedures/OrderStatusShardProcedure.h"
#include "ShardProcedures/StockLevelShardProcedure.h"

namespace Cavalia {
	namespace Benchmark {
		namespace Tpcc {
			namespace Executor {
				using namespace ShardProcedures;
				class TpccHStoreExecutor : public HStoreExecutor {
				public:
					TpccHStoreExecutor(IORedirector *const redirector, BaseStorageManager *const storage_manager, BaseLogger *const logger, const HStoreTxnLocation &txn_location) : HStoreExecutor(redirector, storage_manager, logger, txn_location) {}
					virtual ~TpccHStoreExecutor() {}

				private:
					virtual void PrepareProcedures() {
						registers_[TupleType::DELIVERY] = [](size_t node_id) {
							DeliveryShardProcedure *procedure = (DeliveryShardProcedure*)MemAllocator::AllocNode(sizeof(DeliveryShardProcedure), node_id);
							new(procedure)DeliveryShardProcedure(TupleType::DELIVERY);
							return procedure;
						};
						deregisters_[TupleType::DELIVERY] = [](char *ptr){
							MemAllocator::FreeNode(ptr, sizeof(DeliveryShardProcedure));
						};
						registers_[TupleType::NEW_ORDER] = [](size_t node_id) {
							NewOrderShardProcedure *procedure = (NewOrderShardProcedure*)MemAllocator::AllocNode(sizeof(NewOrderShardProcedure), node_id);
							new(procedure)NewOrderShardProcedure(TupleType::NEW_ORDER);
							return procedure;
						};
						deregisters_[TupleType::NEW_ORDER] = [](char *ptr){
							MemAllocator::FreeNode(ptr, sizeof(NewOrderShardProcedure));
						};
						registers_[TupleType::PAYMENT] = [](size_t node_id) {
							PaymentShardProcedure *procedure = (PaymentShardProcedure*)MemAllocator::AllocNode(sizeof(PaymentShardProcedure), node_id);
							new(procedure)PaymentShardProcedure(TupleType::PAYMENT);
							return procedure;
						};
						deregisters_[TupleType::PAYMENT] = [](char *ptr){
							MemAllocator::FreeNode(ptr, sizeof(PaymentShardProcedure));
						};
						registers_[TupleType::ORDER_STATUS] = [](size_t node_id) {
							OrderStatusShardProcedure *procedure = (OrderStatusShardProcedure*)MemAllocator::AllocNode(sizeof(OrderStatusShardProcedure), node_id);
							new(procedure)OrderStatusShardProcedure(TupleType::ORDER_STATUS);
							return procedure;
						};
						deregisters_[TupleType::ORDER_STATUS] = [](char *ptr){
							MemAllocator::FreeNode(ptr, sizeof(OrderStatusShardProcedure));
						};
						registers_[TupleType::STOCK_LEVEL] = [](size_t node_id) {
							StockLevelShardProcedure *procedure = (StockLevelShardProcedure*)MemAllocator::AllocNode(sizeof(StockLevelShardProcedure), node_id);
							new(procedure)StockLevelShardProcedure(TupleType::STOCK_LEVEL);
							return procedure;
						};
						deregisters_[TupleType::STOCK_LEVEL] = [](char *ptr){
							MemAllocator::FreeNode(ptr, sizeof(StockLevelShardProcedure));
						};
					}

					virtual TxnParam* DeserializeParam(const size_t &param_type, const CharArray &entry) {
						TxnParam *tuple;
						if (param_type == TupleType::DELIVERY) {
							tuple = new DeliveryParam();
						}
						else if (param_type == TupleType::NEW_ORDER) {
							tuple = new NewOrderParam();
						}
						else if (param_type == TupleType::PAYMENT) {
							tuple = new PaymentParam();
						}
						else if (param_type == TupleType::ORDER_STATUS) {
							tuple = new OrderStatusParam();
						}
						else if (param_type == TupleType::STOCK_LEVEL) {
							tuple = new StockLevelParam();
						}
						else {
							assert(false);
							return NULL;
						}
						tuple->Deserialize(entry);
						return tuple;
					}

					virtual void GetPartitionIds(const TxnParam *tuple, std::set<size_t> &part_ids){
						if (tuple->type_ == DELIVERY){
							part_ids.insert((((DeliveryParam*)tuple)->w_id_ - 1) % thread_count_);
						}
						else if (tuple->type_ == NEW_ORDER){
							NewOrderParam *param = (NewOrderParam*)tuple;
							part_ids.insert((param->w_id_ - 1) % thread_count_);
							for (size_t i = 0; i < param->ol_cnt_; ++i){
								part_ids.insert((param->i_w_ids_[i] - 1) % thread_count_);
							}
						}
						else if (tuple->type_ == PAYMENT){
							part_ids.insert((((PaymentParam*)tuple)->w_id_ - 1) % thread_count_);
						}
						else if (tuple->type_ == ORDER_STATUS){
							part_ids.insert((((OrderStatusParam*)tuple)->w_id_ - 1) % thread_count_);
						}
						else if (tuple->type_ == STOCK_LEVEL){
							part_ids.insert((((StockLevelParam*)tuple)->w_id_ - 1) % thread_count_);
						}
					}

				private:
					TpccHStoreExecutor(const TpccHStoreExecutor &);
					TpccHStoreExecutor& operator=(const TpccHStoreExecutor &);
				};
			}
		}
	}
}

#endif
