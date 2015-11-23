#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_TPCC_SOURCE_H__
#define __CAVALIA_TPCC_BENCHMARK_TPCC_SOURCE_H__

#include <BenchmarkSource.h>
#include "TpccInformation.h"
#include "TpccScaleParams.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Tpcc{
			// automatically check whether dump file has already existed. if not, then create new file.
			class TpccSource : public BenchmarkSource{
			public:
				TpccSource(const std::string &filename_prefix, IORedirector *const redirector, TpccScaleParams *const params, const size_t &num_transactions, const SourceType source_type, const size_t &partition_count = 0, const size_t &dist_ratio = 0) : BenchmarkSource(filename_prefix, redirector, params, num_transactions, source_type, partition_count, dist_ratio), scale_params_(params){}
				virtual ~TpccSource(){}

			private:
				virtual void StartExecution();

				DeliveryParam* GenerateDeliveryParam(const int &w_id = -1) const;
				NewOrderParam* GenerateNewOrderParam(const int &w_id = -1) const;
				PaymentParam* GeneratePaymentParam(const int &w_id = -1) const;
				OrderStatusParam* GenerateOrderStatusParam(const int &w_id = -1) const;
				StockLevelParam* GenerateStockLevelParam(const int &w_id = -1) const;

				virtual EventTuple* DeserializeParam(const size_t &param_type, const CharArray &entry) {
					EventTuple *tuple;
					if (param_type == TupleType::DELIVERY) {
						tuple = new DeliveryParam();
					}
					else if (param_type == TupleType::NEW_ORDER) {
						tuple = new NewOrderParam();
					}
					else if (param_type == TupleType::PAYMENT) {
						tuple = new PaymentParam();
					}
					else if (param_type == TupleType::ORDER_STATUS) {
						tuple = new OrderStatusParam();
					}
					else if (param_type == TupleType::STOCK_LEVEL) {
						tuple = new StockLevelParam();
					}
					else {
						assert(false);
						return NULL;
					}
					tuple->Deserialize(entry);
					return tuple;
				}

			private:
				TpccSource(const TpccSource &);
				TpccSource& operator=(const TpccSource &);

			private:
				const TpccScaleParams *const scale_params_;
			};
		}
	}
}

#endif
