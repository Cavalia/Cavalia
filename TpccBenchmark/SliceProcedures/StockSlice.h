#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_REPLAY_SLICES_STOCK_SLICE_H__
#define __CAVALIA_TPCC_BENCHMARK_REPLAY_SLICES_STOCK_SLICE_H__

#include <Transaction/StoredProcedure.h>
#include "../TpccInformation.h"

namespace Cavalia {
	namespace Benchmark {
		namespace Tpcc {
			namespace ReplaySlices {
				using namespace Cavalia::Database;
				class StockSlice : public StoredProcedure {
				public:
					StockSlice() {}
					virtual ~StockSlice() {}

					void Execute(const ParamBatchWrapper *params) {
						for (size_t i = 0; i < params->size(); ++i) {
							NewOrderParam *no_param = static_cast<NewOrderParam*>(params->get(i)->param_);
							size_t part_id = params->get(i)->part_id_;
							// update stocks.
							int ol_i_id = no_param->i_ids_[part_id];
							int ol_supply_w_id = no_param->i_w_ids_[part_id];
							memcpy(s_key, &ol_i_id, sizeof(int));
							memcpy(s_key + sizeof(int), &ol_supply_w_id, sizeof(int));
							// access stock_table
							storage_manager_->tables_[STOCK_TABLE_ID]->SelectKeyRecord(std::string(s_key, sizeof(int) * 2), tb_record);
							SchemaRecord *stock_record = tb_record->record_;
							assert(stock_record != NULL);
							int ol_quantity = no_param->i_qtys_[part_id];
							int ytd = *(int*)(stock_record->GetColumn(13)) + ol_quantity;
							int quantity = *(int*)(stock_record->GetColumn(2));
							if (quantity >= ol_quantity + 10) {
								quantity -= ol_quantity;
							}
							else {
								quantity = quantity + 91 - ol_quantity;
							}
							int order_cnt = *(int*)(stock_record->GetColumn(14)) + 1;
							stock_record->UpdateColumn(13, (char*)(&ytd));
							stock_record->UpdateColumn(2, (char*)(&quantity));
							stock_record->UpdateColumn(14, (char*)(&order_cnt));
							if (ol_supply_w_id != no_param->w_id_) {
								int remote_cnt = *(int*)(stock_record->GetColumn(15)) + 1;
								stock_record->UpdateColumn(15, (char*)(&remote_cnt));
							}

							int dist_column = no_param->d_id_ + 2;
							//stock_record->GetColumn(dist_column, no_param->s_dists_[part_id]);
						}
					}

				private:
					StockSlice(const StockSlice&);
					StockSlice& operator=(const StockSlice&);

				private:
					char s_key[sizeof(int) * 2];
					TableRecord *tb_record;
				};
			}
		}
	}
}

#endif
