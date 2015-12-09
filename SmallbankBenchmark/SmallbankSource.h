#pragma once
#ifndef __CAVALIA_SMALLBANK_BENCHMARK_SMALLBANK_SOURCE_H__
#define __CAVALIA_SMALLBANK_BENCHMARK_SMALLBANK_SOURCE_H__

#include <Benchmark/BenchmarkSource.h>
#include "SmallbankInformation.h"
#include "SmallbankRandomGenerator.h"
#include "SmallbankScaleParams.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Smallbank{
			class SmallbankSource : public BenchmarkSource{
			public:
				SmallbankSource(const std::string &filename_prefix, IORedirector *const redirector, SmallbankScaleParams *const params, const size_t &num_transactions, const size_t &dist_ratio) : BenchmarkSource(filename_prefix, redirector, params, num_transactions, dist_ratio), num_accounts_(static_cast<size_t>(params->scalefactor_ * NUM_ACCOUNTS)), random_generator_(num_accounts_, params->theta_){}

				 SmallbankSource(const std::string &filename_prefix, IORedirector *const redirector, SmallbankScaleParams *const params, const size_t &num_transactions, const size_t &dist_ratio, const size_t &partition_count) : BenchmarkSource(filename_prefix, redirector, params, num_transactions, dist_ratio, partition_count), num_accounts_(static_cast<size_t>(params->scalefactor_ * NUM_ACCOUNTS)), random_generator_(num_accounts_, params->theta_){}

				 SmallbankSource(const std::string &filename_prefix, IORedirector *const redirector, SmallbankScaleParams *const params, const size_t &num_transactions, const size_t &dist_ratio, const size_t &partition_count, const size_t &partition_id) : BenchmarkSource(filename_prefix, redirector, params, num_transactions, dist_ratio, partition_count, partition_id), num_accounts_(static_cast<size_t>(params->scalefactor_ * NUM_ACCOUNTS)), random_generator_(num_accounts_, params->theta_){}

				 virtual ~SmallbankSource(){}

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
				SmallbankSource(const SmallbankSource &);
				SmallbankSource& operator=(const SmallbankSource &);

			private:
				const size_t num_accounts_;
				SmallbankRandomGenerator random_generator_;
			};
		}
	}
}

#endif