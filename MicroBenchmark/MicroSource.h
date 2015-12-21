#pragma once
#ifndef __CAVALIA_MICRO_BENCHMARK_MICRO_SOURCE_H__
#define __CAVALIA_MICRO_BENCHMARK_MICRO_SOURCE_H__

#include <Benchmark/BenchmarkSource.h>
#include "MicroInformation.h"
#include "MicroRandomGenerator.h"
#include "MicroScaleParams.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Micro{
			class MicroSource : public BenchmarkSource{
			public:
				MicroSource(const std::string &filename_prefix, IORedirector *const redirector, MicroScaleParams *const params, const size_t &num_transactions, const size_t &dist_ratio) : BenchmarkSource(filename_prefix, redirector, params, num_transactions, dist_ratio), num_accounts_(static_cast<size_t>(params->scalefactor_ * NUM_ACCOUNTS)), random_generator_(num_accounts_, params->theta_){}

				 MicroSource(const std::string &filename_prefix, IORedirector *const redirector, MicroScaleParams *const params, const size_t &num_transactions, const size_t &dist_ratio, const size_t &partition_count) : BenchmarkSource(filename_prefix, redirector, params, num_transactions, dist_ratio, partition_count), num_accounts_(static_cast<size_t>(params->scalefactor_ * NUM_ACCOUNTS)), random_generator_(num_accounts_, params->theta_){}

				 MicroSource(const std::string &filename_prefix, IORedirector *const redirector, MicroScaleParams *const params, const size_t &num_transactions, const size_t &dist_ratio, const size_t &partition_count, const size_t &partition_id) : BenchmarkSource(filename_prefix, redirector, params, num_transactions, dist_ratio, partition_count, partition_id), num_accounts_(static_cast<size_t>(params->scalefactor_ * NUM_ACCOUNTS)), random_generator_(num_accounts_, params->theta_){}

				 virtual ~MicroSource(){}

			private:
				virtual void StartGeneration();

				virtual TxnParam* DeserializeParam(const size_t &param_type, const CharArray &entry){
					TxnParam *tuple;
					if (param_type == TupleType::AMALGAMATE){
						tuple = new AmalgamateParam();
					}
					else if (param_type == TupleType::DEPOSIT_CHECKING){
						tuple = new DepositCheckingParam();
					}
					else if (param_type == TupleType::SEND_PAYMENT){
						tuple = new SendPaymentParam();
					}
					else if (param_type == TupleType::TRANSACT_SAVINGS){
						tuple = new TransactSavingsParam();
					}
					else if (param_type == TupleType::WRITE_CHECK){
						tuple = new WriteCheckParam();
					}
					else if (param_type == TupleType::BALANCE){
						tuple = new BalanceParam();
					}
					else{
						assert(false);
						return NULL;
					}
					tuple->Deserialize(entry);
					return tuple;
				}

			private:
				MicroSource(const MicroSource &);
				MicroSource& operator=(const MicroSource &);

			private:
				const size_t num_accounts_;
				MicroRandomGenerator random_generator_;
			};
		}
	}
}

#endif