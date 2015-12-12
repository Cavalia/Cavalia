#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_SLICE_DISTRICT_9_SLICE_H__
#define __CAVALIA_TPCC_BENCHMARK_SLICE_DISTRICT_9_SLICE_H__

#include <Transaction/StoredProcedure.h>
#include "../TpccInformation.h"

namespace Cavalia {
	namespace Benchmark {
		namespace Tpcc {
			namespace ReplaySlices {
				using namespace Cavalia::Database;
				class District9Slice : public StoredProcedure {
				public:
					District9Slice() {}
					virtual ~District9Slice() {}

					void Execute(const ParamBatch *params) {
						for (size_t i = 0; i < params->size(); ++i) {
							const PaymentParam *payment_param = static_cast<const PaymentParam*>(params->get(i));
							memcpy(d_key, &payment_param->d_id_, sizeof(int));
							memcpy(d_key + sizeof(int), &payment_param->w_id_, sizeof(int));
							// access district_table
							storage_manager_->tables_[DISTRICT_TABLE_ID]->SelectKeyRecord(std::string(d_key, sizeof(int)* 2), tb_record);
							assert(tb_record != NULL);
							SchemaRecord *district_record = tb_record->record_;
							double d_ytd = *(double*)(district_record->GetColumn(9)) + payment_param->h_amount_;
							district_record->UpdateColumn(9, (char*)(&d_ytd));
						}
					}

				private:
					District9Slice(const District9Slice&);
					District9Slice& operator=(const District9Slice&);

				private:
					char d_key[sizeof(int) * 2];
					TableRecord *tb_record;
				};
			}
		}
	}
}

#endif
