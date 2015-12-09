#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_TPCC_SOURCE_H__
#define __CAVALIA_TPCC_BENCHMARK_TPCC_SOURCE_H__

#include <Benchmark/BenchmarkSource.h>
#include "TpccInformation.h"
#include "TpccScaleParams.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Tpcc{
			// automatically check whether dump file has already existed. if not, then create new file.
			class TpccSource : public BenchmarkSource{
			public:
				TpccSource(const std::string &filename_prefix, IORedirector *const redirector, TpccScaleParams *const params, const size_t &num_transactions, const size_t &dist_ratio) : BenchmarkSource(filename_prefix, redirector, params, num_transactions, dist_ratio), scale_params_(params){}

				TpccSource(const std::string &filename_prefix, IORedirector *const redirector, TpccScaleParams *const params, const size_t &num_transactions, const size_t &dist_ratio, const size_t &partition_count) : BenchmarkSource(filename_prefix, redirector, params, num_transactions, dist_ratio, partition_count), scale_params_(params){}

				TpccSource(const std::string &filename_prefix, IORedirector *const redirector, TpccScaleParams *const params, const size_t &num_transactions, const size_t &dist_ratio, const size_t &partition_count, const size_t &partition_id) : BenchmarkSource(filename_prefix, redirector, params, num_transactions, dist_ratio, partition_count, partition_id), scale_params_(params){}
				virtual ~TpccSource(){}

			private:
				virtual void StartGeneration();

				DeliveryParam* GenerateDeliveryParam(const int &w_id = -1) const;
				NewOrderParam* GenerateNewOrderParam(const int &w_id = -1) const;
				PaymentParam* GeneratePaymentParam(const int &w_id = -1) const;
				OrderStatusParam* GenerateOrderStatusParam(const int &w_id = -1) const;
				StockLevelParam* GenerateStockLevelParam(const int &w_id = -1) const;

				virtual TxnParam* DeserializeParam(const size_t &param_type, const CharArray &entry) {
					TxnParam *tuple;
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
