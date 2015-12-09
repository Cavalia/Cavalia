#pragma once
#ifndef __CAVALIA_SMALLBANK_BENCHMARK_SMALLBANK_META_H__
#define __CAVALIA_SMALLBANK_BENCHMARK_SMALLBANK_META_H__

namespace Cavalia{
	namespace Benchmark{
		namespace Smallbank{
			enum TupleType : size_t { 
				AMALGAMATE, 
				BALANCE, 
				DEPOSIT_CHECKING, 
				SEND_PAYMENT, 
				TRANSACT_SAVINGS, 
				WRITE_CHECK 
			};
			
			enum TableType : size_t { 
				ACCOUNTS_TABLE_ID, 
				SAVINGS_TABLE_ID, 
				CHECKING_TABLE_ID, 
				kTableCount 
			};
		}
	}
}

#endif


