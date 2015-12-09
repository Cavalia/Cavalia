#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_TPCC_POPULATOR_H__
#define __CAVALIA_TPCC_BENCHMARK_TPCC_POPULATOR_H__

#include <Benchmark/BenchmarkPopulator.h>
#include "TpccRecords.h"
#include "TpccScaleParams.h"
#include "TpccInformation.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Tpcc{
			class TpccPopulator : public BenchmarkPopulator{
			public:
				TpccPopulator(const TpccScaleParams *params, BaseStorageManager *storage_manager) : BenchmarkPopulator(storage_manager), scale_params_(params){}
				virtual ~TpccPopulator(){}

				virtual void StartPopulate();
				virtual void StartPopulate(const size_t &min_w_id, const size_t &max_w_id);

			private:
				ItemRecord* GenerateItemRecord(const int &item_id, bool original) const;
				WarehouseRecord* GenerateWarehouseRecord(const int &w_id) const;
				DistrictRecord* GenerateDistrictRecord(const int &d_w_id, const int &d_id, const int &d_next_o_id) const;
				CustomerRecord* GenerateCustomerRecord(const int &c_w_id, const int &c_d_id, const int &c_id, bool bad_credit) const;
				StockRecord* GenerateStockRecord(const int &s_w_id, const int &s_i_id, bool original) const;
				OrderRecord* GenerateOrderRecord(const int &o_w_id, const int &o_d_id, const int &o_id, const int &o_c_id, const int &o_ol_cnt, bool new_order) const;
				NewOrderRecord* GenerateNewOrderRecord(const int &w_id, const int &d_id, const int &o_id) const;
				OrderLineRecord* GenerateOrderLineRecord(const int &ol_w_id, const int &ol_d_id, const int &ol_o_id, const int &ol_number, const int &max_items, bool new_order) const;
				HistoryRecord* GenerateHistoryRecord(const int &h_c_w_id, const int &h_c_d_id, const int &h_c_id) const;
				DistrictNewOrderRecord* GenerateDistrictNewOrderRecord(const int &w_id, const int &d_id, const int &o_id) const;

				void InsertItemRecord(const ItemRecord*);
				void InsertWarehouseRecord(const WarehouseRecord*);
				void InsertDistrictRecord(const DistrictRecord*);
				void InsertCustomerRecord(const CustomerRecord*);
				void InsertStockRecord(const StockRecord*);
				void InsertOrderRecord(const OrderRecord*);
				void InsertNewOrderRecord(const NewOrderRecord*);
				void InsertOrderLineRecord(const OrderLineRecord*);
				void InsertHistoryRecord(const HistoryRecord*);
				void InsertDistrictNewOrderRecord(const DistrictNewOrderRecord*);

			private:
				TpccPopulator(const TpccPopulator&);
				TpccPopulator& operator=(const TpccPopulator&);

			private:
				const TpccScaleParams *const scale_params_;

			};
		}
	}
}

#endif
