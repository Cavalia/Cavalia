#include "SmallbankSource.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Smallbank{
			void SmallbankSource::StartGeneration() {
				double workload_weights[6];
				workload_weights[0] = FREQUENCY_AMALGAMATE;
				workload_weights[1] = FREQUENCY_DEPOSIT_CHECKING;
				workload_weights[2] = FREQUENCY_SEND_PAYMENT;
				workload_weights[3] = FREQUENCY_TRANSACT_SAVINGS;
				workload_weights[4] = FREQUENCY_WRITE_CHECK;
				workload_weights[5] = FREQUENCY_BALANCE;
				double total = 0;
				for (size_t i = 0; i < 6; ++i){
					total += workload_weights[i];
				}
				for (size_t i = 0; i < 6; ++i){
					workload_weights[i] = workload_weights[i] * 1.0 / total * 100;
				}
				for (size_t i = 1; i < 6; ++i){
					workload_weights[i] += workload_weights[i - 1];
				}

				int* acct_cnt = new int[num_accounts_ + 1];
				memset(acct_cnt, 0, sizeof(int) * (num_accounts_ + 1));
				size_t partition_id = 0;
				ParamBatch *tuples = new ParamBatch();
				for (size_t i = 0; i < num_transactions_; ++i){
					int x = SmallbankRandomGenerator::GenerateInteger(1, 100);
					if (x <= workload_weights[0]){
						AmalgamateParam* param = new AmalgamateParam();
						int res1 = 0;
						int res2 = 0;
						if (source_type_ == RANDOM_SOURCE){
							res1 = random_generator_.GenerateZipfNumber();
							res2 = random_generator_.GenerateZipfNumber();
							while (res1 == res2) res2 = random_generator_.GenerateZipfNumber();
						}
						else if(source_type_ == PARTITION_SOURCE){
							while (1){
								res1 = SmallbankRandomGenerator::GenerateInteger(1, num_accounts_);
								if ((res1 - 1) % partition_count_ == partition_id){
									break;
								}
							}
							bool remote = (SmallbankRandomGenerator::GenerateInteger(1, 100) <= (int)dist_ratio_);
							while (1){
								res2 = SmallbankRandomGenerator::GenerateInteger(1, num_accounts_);
								if (remote == true){
									if ((res2 - 1) % partition_count_ != partition_id){
										break;
									}
								}
								else{
									if ((res2 - 1) % partition_count_ == partition_id && res2 != res1){
										break;
									}
								}
							}
						}
						param->custid_0_ = static_cast<int64_t>(res1);
						param->custid_1_ = static_cast<int64_t>(res2);
						tuples->push_back(param);
						acct_cnt[res1]++;
						acct_cnt[res2]++;
					}
					else if (x <= workload_weights[1]){
						DepositCheckingParam* param = new DepositCheckingParam();
						int res = 0;
						if (source_type_ == RANDOM_SOURCE){
							res = random_generator_.GenerateZipfNumber();
						}
						else if(source_type_ == PARTITION_SOURCE){
							while (1){
								res = SmallbankRandomGenerator::GenerateInteger(1, num_accounts_);
								if ((res - 1) % partition_count_ == partition_id){
									break;
								}
							}
						}
						param->custid_ = static_cast<int64_t>(res);
						param->amount_ = 1.3f; // hardcoded in original code
						tuples->push_back(param);
						acct_cnt[res]++;
					}
					else if (x <= workload_weights[2]){
						SendPaymentParam* param = new SendPaymentParam();
						int res1 = 0;
						int res2 = 0;
						if (source_type_ == RANDOM_SOURCE){
							res1 = random_generator_.GenerateZipfNumber();
							res2 = random_generator_.GenerateZipfNumber();
							while (res1 == res2) res2 = random_generator_.GenerateZipfNumber();
						}
						else if(source_type_ == PARTITION_SOURCE){
							while (1){
								res1 = SmallbankRandomGenerator::GenerateInteger(1, num_accounts_);
								if ((res1 - 1) % partition_count_ == partition_id){
									break;
								}
							}
							bool remote = (SmallbankRandomGenerator::GenerateInteger(1, 100) <= (int)dist_ratio_);
							while (1){
								res2 = SmallbankRandomGenerator::GenerateInteger(1, num_accounts_);
								if (remote == true){
									if ((res2 - 1) % partition_count_ != partition_id){
										break;
									}
								}
								else{
									if ((res2 - 1) % partition_count_ == partition_id && res2 != res1){
										break;
									}
								}
							}
						}
						param->custid_0_ = static_cast<int64_t>(res1);
						param->custid_1_ = static_cast<int64_t>(res2);
						param->amount_ = 5.0;
						tuples->push_back(param);
						acct_cnt[res1]++;
						acct_cnt[res2]++;
					}
					else if (x <= workload_weights[3]){
						TransactSavingsParam* param = new TransactSavingsParam();
						int res = 0;
						if (source_type_ == RANDOM_SOURCE){
							res = random_generator_.GenerateZipfNumber();
						}
						else if(source_type_ == PARTITION_SOURCE){
							while (1){
								res = SmallbankRandomGenerator::GenerateInteger(1, num_accounts_);
								if ((res - 1) % partition_count_ == partition_id){
									break;
								}
							}
						}
						param->custid_ = static_cast<int64_t>(res);
						param->amount_ = 20.20f;
						tuples->push_back(param);
						acct_cnt[res]++;
					}
					else if (x <= workload_weights[4]){
						WriteCheckParam* param = new WriteCheckParam();
						int res = 0;
						if (source_type_ == RANDOM_SOURCE){
							res = random_generator_.GenerateZipfNumber();
						}
						else if(source_type_ == PARTITION_SOURCE){
							while (1){
								res = SmallbankRandomGenerator::GenerateInteger(1, num_accounts_);
								if ((res - 1) % partition_count_ == partition_id){
									break;
								}
							}
						}
						param->custid_ = static_cast<int64_t>(res);
						param->amount_ = 5.0f;
						tuples->push_back(param);
						acct_cnt[res]++;
					}
					else {
						BalanceParam* param = new BalanceParam();
						int res = 0;
						if (source_type_ == RANDOM_SOURCE){
							res = random_generator_.GenerateZipfNumber();
						}
						else if(source_type_ == PARTITION_SOURCE){
							while (1){
								res = SmallbankRandomGenerator::GenerateInteger(1, num_accounts_);
								if ((res - 1) % partition_count_ == partition_id){
									break;
								}
							}
						}
						param->custid_ = static_cast<int64_t>(res);
						tuples->push_back(param);
						acct_cnt[res]++;
					}
					if ((i + 1) % gParamBatchSize == 0){
						DumpToDisk(tuples);
						redirector_ptr_->PushParameterBatch(tuples);
						tuples = new ParamBatch();
						if (source_type_ == PARTITION_SOURCE){
							partition_id = (partition_id + 1) % partition_count_;
						}
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
