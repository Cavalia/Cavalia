#pragma once
#ifndef __CAVALIA_DATABASE_BATCH_TIMESTAMP_H__
#define __CAVALIA_DATABASE_BATCH_TIMESTAMP_H__

#include <cstdint>
#include "../Meta/MetaTypes.h"

namespace Cavalia{
	namespace Database{
		struct BatchTimestamp{
			BatchTimestamp(){
				curr_ts_ = 0;
				max_ts_ = 0;
			}
			
			bool IsAvailable(){
				if (curr_ts_ < max_ts_ - 1){
					return true;
				}
				return false;
			}

			int64_t GetTimestamp(){
				int64_t ret_ts = curr_ts_;
				++curr_ts_;
				return ret_ts;
			}
			
			void InitTimestamp(const int64_t &timestamp){
				curr_ts_ = timestamp;
				max_ts_ = timestamp + kBatchTsNum;
			}

			int64_t curr_ts_;
			int64_t max_ts_;
		};
	}
}

#endif