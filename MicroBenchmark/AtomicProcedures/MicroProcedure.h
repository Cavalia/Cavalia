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
						SchemaRecord *record_0 = NULL;
						DB_QUERY(SelectKeyRecord(&context_, MICRO_TABLE_ID, std::string((char*)(&micro_param->key_0_), sizeof(int64_t)), record_0, READ_WRITE));
						SchemaRecord *record_1 = NULL;
						DB_QUERY(SelectKeyRecord(&context_, MICRO_TABLE_ID, std::string((char*)(&micro_param->key_1_), sizeof(int64_t)), record_1, READ_WRITE));
						assert(record_0 != NULL);
						assert(record_1 != NULL);
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
