#pragma once
#ifndef __CAVALIA_MICRO_BENCHMARK_MICRO_CONCURRENT_EXECUTOR_H__
#define __CAVALIA_MICRO_BENCHMARK_MICRO_CONCURRENT_EXECUTOR_H__

#include <Executor/ConcurrentExecutor.h>

#include "AtomicProcedures/AmalgamateProcedure.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Micro{
			namespace Executor{
				class MicroConcurrentExecutor : public ConcurrentExecutor{
				public:
					MicroConcurrentExecutor(IORedirector *const redirector, BaseStorageManager *const storage_manager, BaseLogger *const logger, const size_t &thread_num) : ConcurrentExecutor(redirector, storage_manager, logger, thread_num){}
					virtual ~MicroConcurrentExecutor(){}

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
					}

					virtual TxnParam* DeserializeParam(const size_t &param_type, const CharArray &entry){
						TxnParam *tuple;
						if (param_type == TupleType::AMALGAMATE){
							tuple = new AmalgamateParam();
						}
						else{
							assert(false);
							return NULL;
						}
						tuple->Deserialize(entry);
						return tuple;
					}

				private:
					MicroConcurrentExecutor(const MicroConcurrentExecutor &);
					MicroConcurrentExecutor& operator=(const MicroConcurrentExecutor &);
				};
			}
		}
	}
}

#endif
