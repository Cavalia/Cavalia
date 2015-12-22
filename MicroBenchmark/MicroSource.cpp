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

				int* count = new int[num_items_ + 1];
				memset(count, 0, sizeof(int)* (num_items_ + 1));
				std::unordered_set<int> keys;
				ParamBatch *tuples = new ParamBatch();
				for (size_t i = 0; i < num_transactions_; ++i){
					int x = MicroRandomGenerator::GenerateInteger(1, 100);
					if (x <= workload_weights[0]){
						MicroParam* param = new MicroParam();
						for (size_t access_id = 0; access_id < NUM_ACCESSES; ++access_id){
							int res = random_generator_.GenerateZipfNumber();
							while (keys.find(res) != keys.end()){
								res = random_generator_.GenerateZipfNumber();
							}
							keys.insert(res);
							param->keys_[access_id] = static_cast<int64_t>(res);
							count[res]++;
						}
						tuples->push_back(param);
						keys.clear();
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
				//	std::cout << i << "," << count[i] << std::endl;
				//}
			}
		}
	}
}
