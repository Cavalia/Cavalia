#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_REPLAY_SLICES_ORDER_SLICE_H__
#define __CAVALIA_TPCC_BENCHMARK_REPLAY_SLICES_ORDER_SLICE_H__

#include <unordered_map>
#include <Transaction/StoredProcedure.h>
#include "../TpccInformation.h"

namespace Cavalia {
	namespace Benchmark {
		namespace Tpcc {
			namespace ReplaySlices {
				using namespace Cavalia::Database;
				class OrderSlice : public StoredProcedure {
				public:
					OrderSlice() {}
					virtual ~OrderSlice() {}

					// access order_table.
					void Execute(const ParamBatch *params){
						for (size_t i = 0; i < params->size(); ++i){
							if (params->get(i)->type_ == DELIVERY){
								DeliveryParam *delivery_param = static_cast<DeliveryParam*>(params->get(i));
								for (int d_id = 1; d_id <= DISTRICTS_PER_WAREHOUSE; ++d_id){
									if (delivery_param->no_o_ids_[d_id - 1] == -1){
										continue;
									}
									memcpy(o_key, &(delivery_param->no_o_ids_[d_id - 1]), sizeof(int));
									memcpy(o_key + sizeof(int), &d_id, sizeof(int));
									memcpy(o_key + sizeof(int)+sizeof(int), &(delivery_param->w_id_), sizeof(int));
									// access order_table.
									storage_manager_->tables_[ORDER_TABLE_ID]->SelectKeyRecord(std::string(o_key, sizeof(int)* 3), tb_record);
									assert(tb_record != NULL);
									SchemaRecord *order_record = tb_record->record_;
									assert(order_record != NULL);
									order_record->UpdateColumn(5, (char*)(&delivery_param->o_carrier_id_));
									// read customer id.
									delivery_param->c_ids_[d_id - 1] = *(int*)(order_record->GetColumn(1));
								}
							}
							else if (params->get(i)->type_ == NEW_ORDER){
								const NewOrderParam *no_param = static_cast<const NewOrderParam*>(params->get(i));
								bool all_local = true;
								for (auto & w_id : no_param->i_w_ids_){
									all_local = (all_local && (no_param->w_id_ == w_id));
								}
								char *data = MemAllocator::Alloc(TpccSchema::GenerateOrderSchema()->GetSchemaSize());
								SchemaRecord *order_record = new SchemaRecord(TpccSchema::GenerateOrderSchema(), data);
								order_record->SetColumn(0, (char*)(&no_param->next_o_id_));
								order_record->SetColumn(1, (char*)(&no_param->c_id_));
								order_record->SetColumn(2, (char*)(&no_param->d_id_));
								order_record->SetColumn(3, (char*)(&no_param->w_id_));
								order_record->SetColumn(4, (char*)(&no_param->o_entry_d_));
								order_record->SetColumn(5, (char*)(&NULL_CARRIER_ID));
								order_record->SetColumn(6, (char*)(&no_param->ol_cnt_));
								order_record->SetColumn(7, (char*)(&all_local));
								memcpy(o_key, &no_param->next_o_id_, sizeof(int));
								memcpy(o_key + sizeof(int), &(no_param->d_id_), sizeof(int));
								memcpy(o_key + sizeof(int) + sizeof(int), &(no_param->w_id_), sizeof(int));
								// access order_table
								//storage_manager_->tables_[ORDER_TABLE_ID]->InsertRecord(std::string(o_key, sizeof(int) * 3), order_record);
							}
						}
					}

				private:
					OrderSlice(const OrderSlice&);
					OrderSlice& operator=(const OrderSlice&);

				private:
					char o_key[sizeof(int) * 3];
					TableRecord *tb_record;
				};
			}
		}
	}
}

#endif
