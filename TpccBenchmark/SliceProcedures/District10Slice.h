#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_REPLAY_SLICES_DISTRICT_10_SLICE_H__
#define __CAVALIA_TPCC_BENCHMARK_REPLAY_SLICES_DISTRICT_10_SLICE_H__

#include <Transaction/StoredProcedure.h>
#include "../TpccInformation.h"

namespace Cavalia {
	namespace Benchmark {
		namespace Tpcc {
			namespace ReplaySlices {
				using namespace Cavalia::Database;
				class District10Slice : public StoredProcedure {
				public:
					District10Slice() {}
					virtual ~District10Slice() {}

					void Execute(const ParamBatch *params) {
						for (size_t i = 0; i < params->size(); ++i) {
							NewOrderParam *no_param = static_cast<NewOrderParam*>(params->get(i));
							// get and update next new order id.
							memcpy(d_key, &(no_param->d_id_), sizeof(int));
							memcpy(d_key + sizeof(int), &(no_param->w_id_), sizeof(int));
							// access district_table
							storage_manager_->tables_[DISTRICT_TABLE_ID]->SelectKeyRecord(std::string(d_key, sizeof(int)* 2), tb_record);
							assert(tb_record != NULL);
							SchemaRecord *district_record = tb_record->record_;
							assert(district_record != NULL);
							int d_next_o_id = *(int*)(district_record->GetColumn(10));
							no_param->next_o_id_ = d_next_o_id;
							int o_id = d_next_o_id + 1;
							district_record->UpdateColumn(10, (char*)(&o_id));
						}
					}

				private:
					District10Slice(const District10Slice&);
					District10Slice& operator=(const District10Slice&);

					char d_key[sizeof(int) * 2];
					TableRecord *tb_record;
				};
			}
		}
	}
}

#endif
