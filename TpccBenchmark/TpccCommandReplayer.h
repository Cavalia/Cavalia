#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_TPCC_COMMAND_REPLAYER_H__
#define __CAVALIA_TPCC_BENCHMARK_TPCC_COMMAND_REPLAYER_H__

#include <Replayer/CommandReplayer.h>

#include "AtomicProcedures/DeliveryProcedure.h"
#include "AtomicProcedures/NewOrderProcedure.h"
#include "AtomicProcedures/PaymentProcedure.h"
#include "AtomicProcedures/OrderStatusProcedure.h"
#include "AtomicProcedures/StockLevelProcedure.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Tpcc{
			namespace Replayer{
				using namespace Cavalia::Database;
				class TpccCommandReplayer : public CommandReplayer{
				public:
					TpccCommandReplayer(const std::string &filename, BaseStorageManager *const storage_manager, const size_t &thread_count) : CommandReplayer(filename, storage_manager, thread_count){}
					virtual ~TpccCommandReplayer(){}

				private:
					virtual void PrepareProcedures(){
						using namespace AtomicProcedures;
						registers_[TupleType::DELIVERY] = []() {
							return new DeliveryProcedure(TupleType::DELIVERY);
						};
						registers_[TupleType::NEW_ORDER] = []() {
							return new NewOrderProcedure(TupleType::NEW_ORDER);
						};
						registers_[TupleType::PAYMENT] = []() {
							return new PaymentProcedure(TupleType::PAYMENT);
						};
						registers_[TupleType::ORDER_STATUS] = []() {
							return new OrderStatusProcedure(TupleType::ORDER_STATUS);
						};
						registers_[TupleType::STOCK_LEVEL] = []() {
							return new StockLevelProcedure(TupleType::STOCK_LEVEL);
						};
					}

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

				private:
					TpccCommandReplayer(const TpccCommandReplayer &);
					TpccCommandReplayer& operator=(const TpccCommandReplayer &);
				};
			}
		}
	}
}

#endif
