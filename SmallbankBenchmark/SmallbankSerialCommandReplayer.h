#pragma once
#ifndef __CAVALIA_SMALLBANK_BENCHMARK_SMALLBANK_SERIAL_COMMAND_REPLAYER_H__
#define __CAVALIA_SMALLBANK_BENCHMARK_SMALLBANK_SERIAL_COMMAND_REPLAYER_H__

#include <Replayer/SerialCommandReplayer.h>

#include "AtomicProcedures/AmalgamateProcedure.h"
#include "AtomicProcedures/BalanceProcedure.h"
#include "AtomicProcedures/DepositCheckingProcedure.h"
#include "AtomicProcedures/SendPaymentProcedure.h"
#include "AtomicProcedures/TransactSavingsProcedure.h"
#include "AtomicProcedures/WriteCheckProcedure.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Smallbank{
			namespace Replayer{
				using namespace Cavalia::Database;
				class SmallbankSerialCommandReplayer : public SerialCommandReplayer{
				public:
					SmallbankSerialCommandReplayer(const std::string &filename, BaseStorageManager *const storage_manager, const size_t &thread_count) : SerialCommandReplayer(filename, storage_manager, thread_count){}
					virtual ~SmallbankSerialCommandReplayer(){}

				private:
					virtual void PrepareProcedures(){
						using namespace AtomicProcedures;
						registers_[TupleType::AMALGAMATE] = [](){
							return new AmalgamateProcedure(TupleType::AMALGAMATE);
						};
						registers_[TupleType::BALANCE] = [](){
							return new BalanceProcedure(TupleType::BALANCE);
						};
						registers_[TupleType::DEPOSIT_CHECKING] = [](){
							return new DepositCheckingProcedure(TupleType::DEPOSIT_CHECKING);
						};
						registers_[TupleType::SEND_PAYMENT] = [](){
							return new SendPaymentProcedure(TupleType::SEND_PAYMENT);
						};
						registers_[TupleType::TRANSACT_SAVINGS] = [](){
							return new TransactSavingsProcedure(TupleType::TRANSACT_SAVINGS);
						};
						registers_[TupleType::WRITE_CHECK] = [](){
							return new WriteCheckProcedure(TupleType::WRITE_CHECK);
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

					virtual RecordSchema *GetRecordSchema(const size_t &table_id){
						RecordSchema *schema_ptr = NULL;
						switch (table_id)
						{
						case ACCOUNTS_TABLE_ID:
							schema_ptr = SmallbankSchema::GenerateAccountsSchema();
							break;
						case SAVINGS_TABLE_ID:
							schema_ptr = SmallbankSchema::GenerateSavingsSchema();
							break;
						case CHECKING_TABLE_ID:
							schema_ptr = SmallbankSchema::GenerateCheckingSchema();
							break;
						default:
							assert(false);
							break;
						}
						return schema_ptr;
					}

				private:
					SmallbankSerialCommandReplayer(const SmallbankSerialCommandReplayer &);
					SmallbankSerialCommandReplayer& operator=(const SmallbankSerialCommandReplayer &);
				};
			}
		}
	}
}

#endif
