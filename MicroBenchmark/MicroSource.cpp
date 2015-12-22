#include "MicroSource.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Micro{
			void MicroSource::StartGeneration() {
				double workload_weights[1];
				workload_weights[0] = FREQUENCY_MICRO;
				double total = 0;
				for (size_t i = 0; i < 1; ++i){
					total += workload_weights[i];
				}
				for (size_t i = 0; i < 1; ++i){
					workload_weights[i] = workload_weights[i] * 1.0 / total * 100;
				}
				for (size_t i = 1; i < 1; ++i){
					workload_weights[i] += workload_weights[i - 1];
				}

				int* acct_cnt = new int[num_items_ + 1];
				memset(acct_cnt, 0, sizeof(int)* (num_items_ + 1));
				ParamBatch *tuples = new ParamBatch();
				for (size_t i = 0; i < num_transactions_; ++i){
					int x = MicroRandomGenerator::GenerateInteger(1, 100);
					if (x <= workload_weights[0]){
						MicroParam* param = new MicroParam();
						int res1 = random_generator_.GenerateZipfNumber();
						int res2 = random_generator_.GenerateZipfNumber();
						while (res1 == res2) res2 = random_generator_.GenerateZipfNumber();
						param->key_0_ = static_cast<int64_t>(res1);
						param->key_1_ = static_cast<int64_t>(res2);
						tuples->push_back(param);
						acct_cnt[res1]++;
						acct_cnt[res2]++;
					}
					if ((i + 1) % gParamBatchSize == 0){
						DumpToDisk(tuples);
						redirector_ptr_->PushParameterBatch(tuples);
						tuples = new ParamBatch();
					}
				}
				if (tuples->size() != 0){
					DumpToDisk(tuples);
					redirector_ptr_->PushParameterBatch(tuples);
				}
				else{
					delete tuples;
					tuples = NULL;
				}
				//std::cout << "access distribution from source:" << std::endl;
				//for (int i = 1; i <= 100; ++i){
				//	std::cout << i << "," << acct_cnt[i] << std::endl;
				//}
			}
		}
	}
}
