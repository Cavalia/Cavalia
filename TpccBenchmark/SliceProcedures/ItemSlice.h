#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_REPLAY_SLICES_ITEM_SLICE_H__
#define __CAVALIA_TPCC_BENCHMARK_REPLAY_SLICES_ITEM_SLICE_H__

#include <Transaction/StoredProcedure.h>
#include "../TpccInformation.h"

namespace Cavalia {
	namespace Benchmark {
		namespace Tpcc {
			namespace ReplaySlices {
				using namespace Cavalia::Database;
				class ItemSlice : public StoredProcedure {
				public:
					ItemSlice() {}
					virtual ~ItemSlice() {}

					void Execute(const ParamBatchWrapper *params) {
						for (size_t i = 0; i < params->size(); ++i) {
							NewOrderParam *no_param = static_cast<NewOrderParam*>(params->get(i)->param_);
							size_t part_id = params->get(i)->part_id_;
							// access item_table
							storage_manager_->tables_[ITEM_TABLE_ID]->SelectKeyRecord(std::string((char*)(&no_param->i_ids_[part_id]), sizeof(int)), tb_record);
							assert(tb_record != NULL);
							SchemaRecord *item_record = tb_record->record_;
							assert(item_record != NULL);
							int ol_quantity = no_param->i_qtys_[part_id];
							double price = *(double*)(item_record->GetColumn(3));
							no_param->ol_amounts_[part_id] = ol_quantity * price;
						}
					}

				private:
					ItemSlice(const ItemSlice&);
					ItemSlice& operator=(const ItemSlice&);

				private:
					TableRecord *tb_record;
				};
			}
		}
	}
}

#endif
