#pragma once
#ifndef __CAVALIA_SMALLBANK_BENCHMARK_SMALLBANK_CONCURRENT_EXECUTOR_H__
#define __CAVALIA_SMALLBANK_BENCHMARK_SMALLBANK_CONCURRENT_EXECUTOR_H__

#include <Executor/ConcurrentExecutor.h>

#include "AtomicProcedures/AmalgamateProcedure.h"
#include "AtomicProcedures/BalanceProcedure.h"
#include "AtomicProcedures/DepositCheckingProcedure.h"
#include "AtomicProcedures/SendPaymentProcedure.h"
#include "AtomicProcedures/TransactSavingsProcedure.h"
#include "AtomicProcedures/WriteCheckProcedure.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Smallbank{
			namespace Executor{
				class SmallbankConcurrentExecutor : public ConcurrentExecutor{
				public:
					SmallbankConcurrentExecutor(IORedirector *const redirector, BaseStorageManager *const storage_manager, BaseLogger *const logger, const size_t &thread_num) : ConcurrentExecutor(redirector, storage_manager, logger, thread_num){}
					virtual ~SmallbankConcurrentExecutor(){}

				private:
					virtual void PrepareProcedures(){
						using namespace AtomicProcedures;
						registers_[TupleType::AMALGAMATE] = [](size_t node_id){
							AmalgamateProcedure *procedure = (AmalgamateProcedure*)MemAllocator::AllocNode(sizeof(AmalgamateProcedure), node_id);
							new(procedure)AmalgamateProcedure(TupleType::AMALGAMATE);
							return procedure;
						};
						deregisters_[TupleType::AMALGAMATE] = [](char *ptr){
							MemAllocator::FreeNode(ptr, sizeof(AmalgamateProcedure));
						};
						registers_[TupleType::BALANCE] = [](size_t node_id){
							BalanceProcedure *procedure = (BalanceProcedure*)MemAllocator::AllocNode(sizeof(BalanceProcedure), node_id);
							new(procedure)BalanceProcedure(TupleType::BALANCE);
							return procedure;
						};
						deregisters_[TupleType::BALANCE] = [](char *ptr){
							MemAllocator::FreeNode(ptr, sizeof(BalanceProcedure));
						};
						registers_[TupleType::DEPOSIT_CHECKING] = [](size_t node_id){
							DepositCheckingProcedure *procedure = (DepositCheckingProcedure*)MemAllocator::AllocNode(sizeof(DepositCheckingProcedure), node_id);
							new(procedure)DepositCheckingProcedure(TupleType::DEPOSIT_CHECKING);
							return procedure;
						};
						deregisters_[TupleType::DEPOSIT_CHECKING] = [](char *ptr){
							MemAllocator::FreeNode(ptr, sizeof(DepositCheckingProcedure));
						};
						registers_[TupleType::SEND_PAYMENT] = [](size_t node_id){
							SendPaymentProcedure *procedure = (SendPaymentProcedure*)MemAllocator::AllocNode(sizeof(SendPaymentProcedure), node_id);
							new(procedure)SendPaymentProcedure(TupleType::SEND_PAYMENT);
							return procedure;
						};
						deregisters_[TupleType::SEND_PAYMENT] = [](char *ptr){
							MemAllocator::FreeNode(ptr, sizeof(SendPaymentProcedure));
						};
						registers_[TupleType::TRANSACT_SAVINGS] = [](size_t node_id){
							TransactSavingsProcedure *procedure = (TransactSavingsProcedure*)MemAllocator::AllocNode(sizeof(TransactSavingsProcedure), node_id);
							new(procedure)TransactSavingsProcedure(TupleType::TRANSACT_SAVINGS);
							return procedure;
						};
						deregisters_[TupleType::TRANSACT_SAVINGS] = [](char *ptr){
							MemAllocator::FreeNode(ptr, sizeof(TransactSavingsProcedure));
						};
						registers_[TupleType::WRITE_CHECK] = [](size_t node_id){
							WriteCheckProcedure *procedure = (WriteCheckProcedure*)MemAllocator::AllocNode(sizeof(WriteCheckProcedure), node_id);
							new(procedure)WriteCheckProcedure(TupleType::WRITE_CHECK);
							return procedure;
						};
						deregisters_[TupleType::WRITE_CHECK] = [](char *ptr){
							MemAllocator::FreeNode(ptr, sizeof(WriteCheckProcedure));
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
					SmallbankConcurrentExecutor(const SmallbankConcurrentExecutor &);
					SmallbankConcurrentExecutor& operator=(const SmallbankConcurrentExecutor &);
				};
			}
		}
	}
}

#endif
