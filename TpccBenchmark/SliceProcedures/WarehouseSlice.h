#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_REPLAY_SLICES_WAREHOUSE_SLICE_H__
#define __CAVALIA_TPCC_BENCHMARK_REPLAY_SLICES_WAREHOUSE_SLICE_H__

#include <Transaction/StoredProcedure.h>
#include "../TpccInformation.h"

namespace Cavalia {
	namespace Benchmark {
		namespace Tpcc {
			namespace ReplaySlices {
				using namespace Cavalia::Database;
				class WarehouseSlice : public StoredProcedure {
				public:
					WarehouseSlice() {}
					virtual ~WarehouseSlice() {}

					void Execute(const ParamBatch *params) {
						for (size_t i = 0; i < params->size(); ++i) {
							const PaymentParam *payment_param = static_cast<const PaymentParam*>(params->get(i));
							// access warehouse_table, write warehouse_tyds.
							storage_manager_->tables_[WAREHOUSE_TABLE_ID]->SelectKeyRecord(std::string((char*)(&payment_param->w_id_), sizeof(int)), tb_record);
							SchemaRecord *warehouse_record = tb_record->record_;
							double w_ytd = *(double*)(warehouse_record->GetColumn(8)) + payment_param->h_amount_;
							warehouse_record->UpdateColumn(8, (char*)(&w_ytd));
						}
					}

				private:
					WarehouseSlice(const WarehouseSlice&);
					WarehouseSlice& operator=(const WarehouseSlice&);

				private:
					TableRecord *tb_record;
				};
			}
		}
	}
}

#endif
