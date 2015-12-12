#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_REPLAY_SLICES_HISTORY_SLICE_H__
#define __CAVALIA_TPCC_BENCHMARK_REPLAY_SLICES_HISTORY_SLICE_H__

#include <Transaction/StoredProcedure.h>
#include "../TpccInformation.h"

namespace Cavalia {
	namespace Benchmark {
		namespace Tpcc {
			namespace ReplaySlices {
				using namespace Cavalia::Database;
				class HistorySlice : public StoredProcedure {
				public:
					HistorySlice() {}
					virtual ~HistorySlice() {}

					void Execute(const ParamBatch *params) {
						for (size_t i = 0; i < params->size(); ++i) {
							const PaymentParam *payment_param = static_cast<const PaymentParam*>(params->get(i));
							// insert histories.
							char *data = MemAllocator::Alloc(TpccSchema::GenerateHistorySchema()->GetSchemaSize());
							SchemaRecord *history_record = new SchemaRecord(TpccSchema::GenerateHistorySchema(), data);
							history_record->SetColumn(0, (char*)(&payment_param->c_id_));
							history_record->SetColumn(1, (char*)(&payment_param->c_d_id_));
							history_record->SetColumn(2, (char*)(&payment_param->c_w_id_));
							history_record->SetColumn(3, (char*)(&payment_param->d_id_));
							history_record->SetColumn(4, (char*)(&payment_param->w_id_));
							history_record->SetColumn(5, (char*)(&payment_param->h_date_));
							history_record->SetColumn(6, (char*)(&payment_param->h_amount_));
							// access history_table
							memcpy(h_key, &payment_param->c_id_, sizeof(int));
							memcpy(h_key + sizeof(int), &payment_param->w_id_, sizeof(int));
							memcpy(h_key + sizeof(int) + sizeof(int), &payment_param->h_date_, sizeof(int64_t));
							//storage_manager_->tables_[HISTORY_TABLE_ID]->InsertRecord(h_key, history_record);
						}
					}

				private:
					HistorySlice(const HistorySlice&);
					HistorySlice& operator=(const HistorySlice&);

					char h_key[sizeof(int) + sizeof(int) + sizeof(int64_t)];
				};
			}
		}
	}
}

#endif
