#pragma once
#ifndef __CAVALIA_MICRO_BENCHMARK_MICRO_RECORDS_H__
#define __CAVALIA_MICRO_BENCHMARK_MICRO_RECORDS_H__

#include <cstdint>

namespace Cavalia{
	namespace Benchmark{
		namespace Micro{
			struct AccountsRecord{
				int64_t custid_;
				char name_[64];
			};

			struct SavingsRecord{
				int64_t custid_;
				float bal_;
			};

			struct CheckingRecord{
				int64_t custid_;
				float bal_;
			};
		}
	}
}

#endif
