#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_REPLAY_SLICES_ORDER_LINE_SLICE_H__
#define __CAVALIA_TPCC_BENCHMARK_REPLAY_SLICES_ORDER_LINE_SLICE_H__

#include <unordered_map>
#include <Transaction/StoredProcedure.h>
#include "../TpccInformation.h"

namespace Cavalia {
	namespace Benchmark {
		namespace Tpcc {
			namespace ReplaySlices {
				using namespace Cavalia::Database;
				class OrderLineSlice : public StoredProcedure {
				public:
					OrderLineSlice() {
						tb_records = new TableRecords(15);
					}
					virtual ~OrderLineSlice() {}

					// access order_line_table.
					void Execute(const ParamBatchWrapper *params){
						for (size_t i = 0; i < params->size(); ++i) {
							if (params->get(i)->param_->type_ == DELIVERY) {
								DeliveryParam *delivery_param = static_cast<DeliveryParam*>(params->get(i)->param_);
								size_t part_id = params->get(i)->part_id_;
								if (delivery_param->no_o_ids_[part_id] == -1){
									continue;
								}
								int d_id = part_id + 1;
								// access order_line_table
								memcpy(ol_key, &(delivery_param->no_o_ids_[part_id]), sizeof(int));
								memcpy(ol_key + sizeof(int), &d_id, sizeof(int));
								memcpy(ol_key + sizeof(int) + sizeof(int), &delivery_param->w_id_, sizeof(int));
								double sum = 0;
								tb_records->Clear();
								storage_manager_->tables_[ORDER_LINE_TABLE_ID]->SelectRecords(0, std::string(ol_key, sizeof(int)* 3), tb_records);
								for (size_t ol_i = 0; ol_i < tb_records->curr_size_; ++ol_i) {
									SchemaRecord *ol_record = tb_records->records_[ol_i]->record_;
									assert(ol_record != NULL);
									ol_record->UpdateColumn(6, (char*)(&delivery_param->ol_delivery_d_));
									sum += *(double*)(ol_record->GetColumn(8));
								}
								delivery_param->sums_[part_id] = sum;
								/////////////////
							}
							else if (params->get(i)->param_->type_ == NEW_ORDER){
								const NewOrderParam *no_param = static_cast<const NewOrderParam*>(params->get(i)->param_);
								size_t part_id = params->get(i)->part_id_;
								for (size_t ol_i = 0; ol_i < no_param->ol_cnt_; ++ol_i){
									// insert order lines.
									int ol_number = ol_i + 1;
									int ol_i_id = no_param->i_ids_[ol_i];
									int ol_supply_w_id = no_param->i_w_ids_[ol_i];
									int ol_quantity = no_param->i_qtys_[ol_i];

									char *data = MemAllocator::Alloc(TpccSchema::GenerateOrderLineSchema()->GetSchemaSize());
									SchemaRecord *ol_record = new SchemaRecord(TpccSchema::GenerateOrderLineSchema(), data);
									ol_record->SetColumn(0, (char*)(&no_param->next_o_id_));
									ol_record->SetColumn(1, (char*)(&no_param->d_id_));
									ol_record->SetColumn(2, (char*)(&no_param->w_id_));
									ol_record->SetColumn(3, (char*)(&ol_number));
									ol_record->SetColumn(4, (char*)(&ol_i_id));
									ol_record->SetColumn(5, (char*)(&ol_supply_w_id));
									ol_record->SetColumn(6, (char*)(&no_param->o_entry_d_));
									ol_record->SetColumn(7, (char*)(&ol_quantity));
									ol_record->SetColumn(8, (char*)(&no_param->ol_amounts_[part_id]));
									ol_record->SetColumn(9, no_param->s_dists_[part_id]);
									memcpy(ol_key, &no_param->next_o_id_, sizeof(int));
									memcpy(ol_key + sizeof(int), &no_param->d_id_, sizeof(int));
									memcpy(ol_key + sizeof(int) + sizeof(int), &no_param->w_id_, sizeof(int));
									memcpy(ol_key + sizeof(int) + sizeof(int) + sizeof(int), &ol_number, sizeof(int));
									// access order_line_table
									//storage_manager_->tables_[ORDER_LINE_TABLE_ID]->InsertRecord(std::string(ol_key, sizeof(int) * 4), ol_record);
								}
							}
						}
					}

				private:
					OrderLineSlice(const OrderLineSlice&);
					OrderLineSlice& operator=(const OrderLineSlice&);

				private:
					char ol_key[sizeof(int)* 4];

					TableRecords *tb_records;
				};
			}
		}
	}
}

#endif
