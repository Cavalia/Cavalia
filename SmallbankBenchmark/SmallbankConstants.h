#pragma once
#ifndef __CAVALIA_SMALLBANK_BENCHMARK_SMALLBANK_CONSTANTS_H__
#define __CAVALIA_SMALLBANK_BENCHMARK_SMALLBANK_CONSTANTS_H__

namespace Cavalia{
	namespace Benchmark{
		namespace Smallbank{
			// ----------------------------------------------------------------
			// STORED PROCEDURE EXECUTION FREQUENCIES (0-100)
			// ----------------------------------------------------------------
			const int FREQUENCY_AMALGAMATE = 15;
			const int FREQUENCY_BALANCE =  15;
			const int FREQUENCY_DEPOSIT_CHECKING = 15;
			const int FREQUENCY_SEND_PAYMENT = 25;
			const int FREQUENCY_TRANSACT_SAVINGS = 15;
			const int FREQUENCY_WRITE_CHECK = 15;

			const int MIN_BALANCE = 10000;
			const int MAX_BALANCE = 50000;
			// Default number of customers in bank
			const int NUM_ACCOUNTS = 1000000;
			const int ACCOUNT_NAME_LEN = 64;
		}
	}
}

#endif
