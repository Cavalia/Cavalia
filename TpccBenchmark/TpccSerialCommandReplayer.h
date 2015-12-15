#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_TPCC_SERIAL_COMMAND_REPLAYER_H__
#define __CAVALIA_TPCC_BENCHMARK_TPCC_SERIAL_COMMAND_REPLAYER_H__

#include <Replayer/SerialCommandReplayer.h>

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
				class TpccSerialCommandReplayer : public SerialCommandReplayer{
				public:
					TpccSerialCommandReplayer(const std::string &filename, BaseStorageManager *const storage_manager, const size_t &thread_count) : SerialCommandReplayer(filename, storage_manager, thread_count){}
					virtual ~TpccSerialCommandReplayer(){}

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

					virtual RecordSchema *GetRecordSchema(const size_t &table_id){
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

				private:
					TpccSerialCommandReplayer(const TpccSerialCommandReplayer &);
					TpccSerialCommandReplayer& operator=(const TpccSerialCommandReplayer &);
				};
			}
		}
	}
}

#endif
