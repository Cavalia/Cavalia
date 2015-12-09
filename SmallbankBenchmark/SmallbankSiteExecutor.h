#pragma once
#ifndef __CAVALIA_SMALLBANK_BENCHMARK_SMALLBANK_SITE_EXECUTOR_H__
#define __CAVALIA_SMALLBANK_BENCHMARK_SMALLBANK_SITE_EXECUTOR_H__

#include <Executor/SiteExecutor.h>

#include "ShardProcedures/AmalgamateShardProcedure.h"
#include "ShardProcedures/BalanceShardProcedure.h"
#include "ShardProcedures/DepositCheckingShardProcedure.h"
#include "ShardProcedures/SendPaymentShardProcedure.h"
#include "ShardProcedures/TransactSavingsShardProcedure.h"
#include "ShardProcedures/WriteCheckShardProcedure.h"

namespace Cavalia {
	namespace Benchmark {
		namespace Smallbank {
			namespace Executor {
				using namespace ShardProcedures;
				class SmallbankSiteExecutor : public SiteExecutor {
				public:
					SmallbankSiteExecutor(IORedirector *const redirector, BaseStorageManager *const storage_manager, BaseLogger *const logger, const SiteTxnLocation &txn_location) : SiteExecutor(redirector, storage_manager, logger, txn_location) {}
					virtual ~SmallbankSiteExecutor() {}

				private:
					virtual void PrepareProcedures() {
						registers_[TupleType::AMALGAMATE] = [](size_t node_id) {
							AmalgamateShardProcedure *procedure = (AmalgamateShardProcedure*)MemAllocator::AllocNode(sizeof(AmalgamateShardProcedure), node_id);
							new(procedure)AmalgamateShardProcedure(TupleType::AMALGAMATE);
							return procedure;
						};
						deregisters_[TupleType::AMALGAMATE] = [](char *ptr){
							MemAllocator::FreeNode(ptr, sizeof(AmalgamateShardProcedure));
						};
						registers_[TupleType::BALANCE] = [](size_t node_id) {
							BalanceShardProcedure *procedure = (BalanceShardProcedure*)MemAllocator::AllocNode(sizeof(BalanceShardProcedure), node_id);
							new(procedure)BalanceShardProcedure(TupleType::BALANCE);
							return procedure;
						};
						deregisters_[TupleType::BALANCE] = [](char *ptr){
							MemAllocator::FreeNode(ptr, sizeof(BalanceShardProcedure));
						};
						registers_[TupleType::DEPOSIT_CHECKING] = [](size_t node_id) {
							DepositCheckingShardProcedure *procedure = (DepositCheckingShardProcedure*)MemAllocator::AllocNode(sizeof(DepositCheckingShardProcedure), node_id);
							new(procedure)DepositCheckingShardProcedure(TupleType::DEPOSIT_CHECKING);
							return procedure;
						};
						deregisters_[TupleType::DEPOSIT_CHECKING] = [](char *ptr){
							MemAllocator::FreeNode(ptr, sizeof(DepositCheckingShardProcedure));
						};
						registers_[TupleType::SEND_PAYMENT] = [](size_t node_id) {
							SendPaymentShardProcedure *procedure = (SendPaymentShardProcedure*)MemAllocator::AllocNode(sizeof(SendPaymentShardProcedure), node_id);
							new(procedure)SendPaymentShardProcedure(TupleType::SEND_PAYMENT);
							return procedure;
						};
						deregisters_[TupleType::SEND_PAYMENT] = [](char *ptr){
							MemAllocator::FreeNode(ptr, sizeof(SendPaymentShardProcedure));
						};
						registers_[TupleType::TRANSACT_SAVINGS] = [](size_t node_id) {
							TransactSavingsShardProcedure *procedure = (TransactSavingsShardProcedure*)MemAllocator::AllocNode(sizeof(TransactSavingsShardProcedure), node_id);
							new(procedure)TransactSavingsShardProcedure(TupleType::TRANSACT_SAVINGS);
							return procedure;
						};
						deregisters_[TupleType::TRANSACT_SAVINGS] = [](char *ptr){
							MemAllocator::FreeNode(ptr, sizeof(TransactSavingsShardProcedure));
						};
						registers_[TupleType::WRITE_CHECK] = [](size_t node_id) {
							WriteCheckShardProcedure *procedure = (WriteCheckShardProcedure*)MemAllocator::AllocNode(sizeof(WriteCheckShardProcedure), node_id);
							new(procedure)WriteCheckShardProcedure(TupleType::WRITE_CHECK);
							return procedure;
						};
						deregisters_[TupleType::WRITE_CHECK] = [](char *ptr){
							MemAllocator::FreeNode(ptr, sizeof(WriteCheckShardProcedure));
						};
					}

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
					SmallbankSiteExecutor(const SmallbankSiteExecutor &);
					SmallbankSiteExecutor& operator=(const SmallbankSiteExecutor &);
				};
			}
		}
	}
}

#endif
