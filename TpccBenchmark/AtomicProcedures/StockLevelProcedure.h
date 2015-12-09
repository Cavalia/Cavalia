#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_ATOMIC_PROCEDURES_STOCK_LEVEL_PROCEDURE_H__
#define __CAVALIA_TPCC_BENCHMARK_ATOMIC_PROCEDURES_STOCK_LEVEL_PROCEDURE_H__

#include <Transaction/StoredProcedure.h>
#include "../TpccInformation.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Tpcc{
			namespace AtomicProcedures{
				class StockLevelProcedure : public StoredProcedure{
				public:
					StockLevelProcedure(const size_t &txn_type) : StoredProcedure(txn_type){
						context_.is_read_only_ = true;
						order_line_records = new SchemaRecords(15);
					}
					virtual ~StockLevelProcedure(){}

					virtual bool Execute(TxnParam *param, CharArray &ret, const ExeContext &exe_context){
						context_.PassContext(exe_context);
						const StockLevelParam *stock_level_param = static_cast<const StockLevelParam*>(param);
						memcpy(d_key, &stock_level_param->d_id_, sizeof(int));
						memcpy(d_key + sizeof(int), &stock_level_param->w_id_, sizeof(int));
						SchemaRecord *district_record = NULL;
						// "getOId": "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?"
						DB_QUERY(SelectKeyRecord(&context_, DISTRICT_TABLE_ID, std::string(d_key, sizeof(int)* 2), district_record, READ_ONLY));
						assert(district_record != NULL);
						int d_next_o_id = *(int*)(district_record->GetColumn(10));
						size_t count = 0;
						for (int o_id = d_next_o_id - 5; o_id < d_next_o_id; ++o_id){
							memcpy(ol_key, &o_id, sizeof(o_id));
							memcpy(ol_key + sizeof(int), &stock_level_param->d_id_, sizeof(int));
							memcpy(ol_key + sizeof(int)+sizeof(int), &stock_level_param->w_id_, sizeof(int));
							// "getStockCount": "SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID < ? AND OL_O_ID >= ? AND S_W_ID = ? AND S_I_ID = OL_I_ID AND S_QUANTITY < ?"
							DB_QUERY(SelectRecords(&context_, ORDER_LINE_TABLE_ID, 0, std::string(ol_key, sizeof(int)* 3), order_line_records, READ_ONLY));
							count += order_line_records->curr_size_;
							order_line_records->Clear();
						}
						ret.Memcpy(ret.size_, (char*)(&d_next_o_id), sizeof(int));
						ret.size_ += sizeof(int);
						ret.Memcpy(ret.size_, (char*)(&count), sizeof(size_t));
						ret.size_ += sizeof(size_t);
						return transaction_manager_->CommitTransaction(&context_, param, ret);
					}

				private:
					StockLevelProcedure(const StockLevelProcedure&);
					StockLevelProcedure& operator=(const StockLevelProcedure&);

				private:
					char d_key[sizeof(int)* 2];
					char ol_key[sizeof(int)* 3];
					SchemaRecords *order_line_records;
				};
			}
		}
	}
}

#endif
