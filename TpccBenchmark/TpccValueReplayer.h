#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_TPCC_VALUE_REPLAYER_H__
#define __CAVALIA_TPCC_BENCHMARK_TPCC_VALUE_REPLAYER_H__

#include <Replayer/ValueReplayer.h>
#include "TpccMeta.h"
#include "TpccSchema.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Tpcc{
			namespace Replayer{
				using namespace Cavalia::Database;
				class TpccValueReplayer : public ValueReplayer{
				public:
					TpccValueReplayer(const std::string &filename, BaseStorageManager *const storage_manager, const size_t &thread_count) : ValueReplayer(filename, storage_manager, thread_count){}
					virtual ~TpccValueReplayer(){}

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
					TpccValueReplayer(const TpccValueReplayer &);
					TpccValueReplayer& operator=(const TpccValueReplayer &);
				};
			}
		}
	}
}

#endif