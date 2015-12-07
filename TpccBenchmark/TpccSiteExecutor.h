#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_TPCC_SITE_EXECUTOR_H__
#define __CAVALIA_TPCC_BENCHMARK_TPCC_SITE_EXECUTOR_H__

#include <Executor/SiteExecutor.h>

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
				class TpccSiteExecutor : public SiteExecutor {
				public:
					TpccSiteExecutor(IORedirector *const redirector, BaseStorageManager *const storage_manager, BaseLogger *const logger, const SiteTxnLocation &txn_location) : SiteExecutor(redirector, storage_manager, logger, txn_location) {}
					virtual ~TpccSiteExecutor() {}

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

				private:
					TpccSiteExecutor(const TpccSiteExecutor &);
					TpccSiteExecutor& operator=(const TpccSiteExecutor &);
				};
			}
		}
	}
}

#endif
