#pragma once
#ifndef __CAVALIA_MICRO_BENCHMARK_ATOMIC_PROCEDURES_MICRO_PROCEDURE_H__
#define __CAVALIA_MICRO_BENCHMARK_ATOMIC_PROCEDURES_MICRO_PROCEDURE_H__

#include <Transaction/StoredProcedure.h>
#include "../MicroInformation.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Micro{
			namespace AtomicProcedures{
				class MicroProcedure : public StoredProcedure{
				public:
					MicroProcedure(const size_t &txn_type) : StoredProcedure(txn_type){}
					virtual ~MicroProcedure(){}

					virtual bool Execute(TxnParam *param, CharArray &ret, const ExeContext &exe_context){
						const MicroParam* micro_param = static_cast<const MicroParam*>(param);
						for (size_t i = 0; i < NUM_ACCESSES / 2; ++i){
							SchemaRecord *record = NULL;
							DB_QUERY(SelectKeyRecord(&context_, MICRO_TABLE_ID, std::string((char*)(&micro_param->keys_[i]), sizeof(int64_t)), record, READ_ONLY));
							assert(record != NULL);
						}
						for (size_t i = NUM_ACCESSES / 2; i < NUM_ACCESSES; ++i){
							SchemaRecord *record = NULL;
							DB_QUERY(SelectKeyRecord(&context_, MICRO_TABLE_ID, std::string((char*)(&micro_param->keys_[i]), sizeof(int64_t)), record, READ_WRITE));
							assert(record != NULL);
						}
						return transaction_manager_->CommitTransaction(&context_, param, ret);
					}

				private:
					MicroProcedure(const MicroProcedure&);
					MicroProcedure& operator=(const MicroProcedure&);
				};
			}
		}
	}
}

#endif
