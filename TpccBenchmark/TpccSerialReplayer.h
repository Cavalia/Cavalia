#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_TPCC_SERIAL_REPLAYER_H__
#define __CAVALIA_TPCC_BENCHMARK_TPCC_SERIAL_REPLAYER_H__

#include <SerialReplayer.h>

#include "AtomicProcedures/DeliveryProcedure.h"
#include "AtomicProcedures/NewOrderProcedure.h"
#include "AtomicProcedures/PaymentProcedure.h"
#include "AtomicProcedures/OrderStatusProcedure.h"
#include "AtomicProcedures/StockLevelProcedure.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Tpcc{
			namespace Replayer{
				using namespace AtomicProcedures;
				class TpccSerialReplayer : public SerialReplayer{
				public:
					TpccSerialReplayer(const std::string &filename, BaseStorageManager *const storage_manager) : SerialReplayer(filename, storage_manager){}
					virtual ~TpccSerialReplayer(){}

				private:
					virtual void RegisterProcedures(){
						procedures_[TupleType::DELIVERY] = new DeliveryProcedure(TupleType::DELIVERY);
						procedures_[TupleType::NEW_ORDER] = new NewOrderProcedure(TupleType::NEW_ORDER);
						procedures_[TupleType::PAYMENT] = new PaymentProcedure(TupleType::PAYMENT);
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
					TpccSerialReplayer(const TpccSerialReplayer &);
					TpccSerialReplayer& operator=(const TpccSerialReplayer &);
				};
			}
		}
	}
}

#endif
