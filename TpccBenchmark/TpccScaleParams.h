#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_SCALE_PARAMS_H__
#define __CAVALIA_TPCC_BENCHMARK_SCALE_PARAMS_H__

#include <Benchmark/BenchmarkScaleParams.h>
#include "TpccConstants.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Tpcc{
			class TpccScaleParams : public BenchmarkScaleParams{
			public:
				TpccScaleParams(const int &num_warehouses, const double &scalefactor) : num_warehouses_(num_warehouses), scalefactor_(scalefactor), starting_warehouse_(1), ending_warehouse_(starting_warehouse_ + num_warehouses - 1), num_items_(static_cast<int>(NUM_ITEMS / scalefactor)), num_districts_per_warehouse_(DISTRICTS_PER_WAREHOUSE), num_customers_per_district_(static_cast<int>(CUSTOMERS_PER_DISTRICT / scalefactor)), num_new_orders_per_district_(static_cast<int>(INITIAL_NEW_ORDERS_PER_DISTRICT / scalefactor)){}

				virtual std::string ToString() const {
					std::string ret;
					ret += std::to_string(num_warehouses_);
					ret += "_";
					ret += std::to_string(scalefactor_);
					return ret;
				}

			public:
				const int num_warehouses_;
				const double scalefactor_;
				const int starting_warehouse_;
				const int ending_warehouse_;
				const int num_items_;
				const int num_districts_per_warehouse_;
				const int num_customers_per_district_;
				const int num_new_orders_per_district_;
			};
		}
	}
}

#endif