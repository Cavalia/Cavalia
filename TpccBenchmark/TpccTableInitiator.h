#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_TPCC_TABLE_INITIATOR_H__
#define __CAVALIA_TPCC_BENCHMARK_TPCC_TABLE_INITIATOR_H__

#include <Benchmark/BenchmarkTableInitiator.h>
#include "TpccInformation.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Tpcc{
			class TpccTableInitiator : public BenchmarkTableInitiator{
			public:
				TpccTableInitiator(){}
				virtual ~TpccTableInitiator(){}

				virtual void Initialize(BaseStorageManager *storage_manager){
					std::unordered_map<size_t, RecordSchema*> schemas;
					schemas[ITEM_TABLE_ID] = TpccSchema::GenerateItemSchema();
					schemas[WAREHOUSE_TABLE_ID] = TpccSchema::GenerateWarehouseSchema();
					schemas[DISTRICT_TABLE_ID] = TpccSchema::GenerateDistrictSchema();
					schemas[CUSTOMER_TABLE_ID] = TpccSchema::GenerateCustomerSchema();
					schemas[ORDER_TABLE_ID] = TpccSchema::GenerateOrderSchema();
					schemas[NEW_ORDER_TABLE_ID] = TpccSchema::GenerateNewOrderSchema();
					schemas[ORDER_LINE_TABLE_ID] = TpccSchema::GenerateOrderLineSchema();
					schemas[HISTORY_TABLE_ID] = TpccSchema::GenerateHistorySchema();
					schemas[STOCK_TABLE_ID] = TpccSchema::GenerateStockSchema();
					schemas[DISTRICT_NEW_ORDER_TABLE_ID] = TpccSchema::GenerateDistrictNewOrderSchema();
					storage_manager->RegisterTables(schemas);
				}

			private:
				TpccTableInitiator(const TpccTableInitiator&);
				TpccTableInitiator& operator=(const TpccTableInitiator&);
			};
		}
	}
}

#endif
