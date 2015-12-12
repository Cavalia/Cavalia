#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_REPLAY_SLICES_NEW_ORDER_SLICE_H__
#define __CAVALIA_TPCC_BENCHMARK_REPLAY_SLICES_NEW_ORDER_SLICE_H__

#include <unordered_map>
#include <Transaction/StoredProcedure.h>
#include "../TpccInformation.h"

namespace Cavalia {
	namespace Benchmark {
		namespace Tpcc {
			namespace ReplaySlices {
				using namespace Cavalia::Database;
				class NewOrderSlice : public StoredProcedure {
				public:
					NewOrderSlice() {}
					virtual ~NewOrderSlice() {}

					// access new_order_table.
					void Execute(const ParamBatch *params) {
						for (size_t i = 0; i < params->size(); ++i){
							if (params->get(i)->type_ == DELIVERY){
								DeliveryParam *delivery_param = static_cast<DeliveryParam*>(params->get(i));
								for (int d_id = 1; d_id <= DISTRICTS_PER_WAREHOUSE; ++d_id){
									// access district_new_order_table, get new order id.
									memcpy(d_no_key, &d_id, sizeof(int));
									memcpy(d_no_key + sizeof(int), &(delivery_param->w_id_), sizeof(int));
									storage_manager_->tables_[DISTRICT_NEW_ORDER_TABLE_ID]->SelectKeyRecord(std::string(d_no_key, sizeof(int) * 2), tb_record);
									assert(tb_record != NULL);
									SchemaRecord *dist_no_record = tb_record->record_;
									assert(dist_no_record != NULL);
									int no_o_id = *(int*)(dist_no_record->GetColumn(2));

									// access new_order_table, delete new order id.
									memcpy(no_key, &no_o_id, sizeof(int));
									memcpy(no_key + sizeof(int), &d_id, sizeof(int));
									memcpy(no_key + sizeof(int) + sizeof(int), &(delivery_param->w_id_), sizeof(int));
									storage_manager_->tables_[NEW_ORDER_TABLE_ID]->SelectKeyRecord(std::string(no_key, sizeof(int) * 3), tb_record);
									if (tb_record != NULL) {
										delivery_param->no_o_ids_[d_id - 1] = no_o_id;
										int next_o_id = no_o_id + 1;
										dist_no_record->UpdateColumn(2, (char*)(&next_o_id));
									}
									else{
										delivery_param->no_o_ids_[d_id - 1] = -1;
										int next_o_id = 1;
										dist_no_record->UpdateColumn(2, (char*)(&next_o_id));
									}
								}
							}
							else if (params->get(i)->type_ == NEW_ORDER){
								const NewOrderParam *no_param = static_cast<const NewOrderParam*>(params->get(i));
								// insert new orders.
								char *data = MemAllocator::Alloc(TpccSchema::GenerateNewOrderSchema()->GetSchemaSize());
								SchemaRecord *new_order_record = new SchemaRecord(TpccSchema::GenerateNewOrderSchema(), data);
								new_order_record->SetColumn(0, (char*)(&no_param->next_o_id_));
								new_order_record->SetColumn(1, (char*)(&no_param->d_id_));
								new_order_record->SetColumn(2, (char*)(&no_param->w_id_));
								// access new_order_table
								//storage_manager_->tables_[NEW_ORDER_TABLE_ID]->InsertRecord(std::string(new_order_record->data_ptr_, sizeof(int)*3), new_order_record);
							}
						}
					}

				private:
					NewOrderSlice(const NewOrderSlice&);
					NewOrderSlice& operator=(const NewOrderSlice&);

				private:
					char d_no_key[sizeof(int) + sizeof(int)];
					char no_key[sizeof(int) + sizeof(int) + sizeof(int)];

					TableRecord *tb_record;
				};
			}
		}
	}
}

#endif
