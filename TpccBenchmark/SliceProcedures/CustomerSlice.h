#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_REPLAY_SLICES_CUSTOMER_SLICE_H__
#define __CAVALIA_TPCC_BENCHMARK_REPLAY_SLICES_CUSTOMER_SLICE_H__

#include <unordered_map>
#include <Transaction/StoredProcedure.h>
#include "../TpccInformation.h"

namespace Cavalia {
	namespace Benchmark {
		namespace Tpcc {
			namespace ReplaySlices {
				using namespace Cavalia::Database;
				class Customer16Slice : public StoredProcedure {
				public:
					Customer16Slice() {}
					virtual ~Customer16Slice() {}

					// access customer_table.
					void Execute(const ParamBatch *params) {
						for (size_t i = 0; i < params->size(); ++i) {
							if (params->get(i)->type_ == DELIVERY) {
								const DeliveryParam *delivery_param = static_cast<const DeliveryParam*>(params->get(i));
								// update customers.
								for (int d_id = 1; d_id <= DISTRICTS_PER_WAREHOUSE; ++d_id){
									if (delivery_param->no_o_ids_[d_id - 1] == -1){
										continue;
									}
									// access customer_table.
									memcpy(c_key, &(delivery_param->c_ids_[d_id - 1]), sizeof(int));
									memcpy(c_key + sizeof(int), &d_id, sizeof(int));
									memcpy(c_key + sizeof(int) + sizeof(int), &(delivery_param->w_id_), sizeof(int));
									storage_manager_->tables_[CUSTOMER_TABLE_ID]->SelectKeyRecord(std::string(c_key, sizeof(int)* 3), tb_record);
									assert(tb_record != NULL);
									SchemaRecord *customer_record = tb_record->record_;
									assert(customer_record != NULL);
									double balance = *(double*)(customer_record->GetColumn(16)) + delivery_param->sums_[d_id - 1];
									customer_record->UpdateColumn(16, (char*)(&balance));
								}
							}
							else if (params->get(i)->type_ == PAYMENT) {
								const PaymentParam *payment_param = static_cast<const PaymentParam*>(params->get(i));
								// access customer_table
								memcpy(c_key, &payment_param->c_id_, sizeof(int));
								memcpy(c_key + sizeof(int), &payment_param->d_id_, sizeof(int));
								memcpy(c_key + sizeof(int) + sizeof(int), &payment_param->w_id_, sizeof(int));
								storage_manager_->tables_[CUSTOMER_TABLE_ID]->SelectKeyRecord(std::string(c_key, sizeof(int)* 3), tb_record);
								assert(tb_record != NULL);
								SchemaRecord *customer_record = tb_record->record_;
								double balance = *(double*)(customer_record->GetColumn(16)) - payment_param->h_amount_;
								double ytd_payment = *(double*)(customer_record->GetColumn(17)) + payment_param->h_amount_;
								int payment_cnt = *(int*)(customer_record->GetColumn(18)) + 1;
								customer_record->UpdateColumn(16, (char*)(&balance));
								customer_record->UpdateColumn(17, (char*)(&ytd_payment));
								customer_record->UpdateColumn(18, (char*)(&payment_cnt));
							}
						}
					}

				private:
					Customer16Slice(const Customer16Slice&);
					Customer16Slice& operator=(const Customer16Slice&);

				private:
					char c_key[sizeof(int) * 3];
					TableRecord *tb_record;
				};
			}
		}
	}
}

#endif
