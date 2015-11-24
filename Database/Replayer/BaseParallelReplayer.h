#pragma once
#ifndef __CAVALIA_DATABASE_BASE_PARALLEL_REPLAYER_H__
#define __CAVALIA_DATABASE_BASE_PARALLEL_REPLAYER_H__

#include <boost/lockfree/queue.hpp>
#include <functional>
#include <fstream>
#include <vector>
#include <cassert>
#include "TransactionManager.h"
#include "BaseReplayer.h"
#include "ScheduleGraph.h"
#include "BoostTransactionPool.h"

namespace Cavalia{
	namespace Database{
		class BaseParallelReplayer : public BaseReplayer{
		public:
			BaseParallelReplayer(const std::string &filename, BaseStorageManager *const storage_manager, const size_t &thread_count, const std::string &partition_filename) : BaseReplayer(filename, storage_manager, thread_count), transaction_manager_(storage_manager_, &logger_), transaction_pool_(thread_count), partition_filename_(partition_filename) {}
			virtual ~BaseParallelReplayer(){}

			virtual void ProcessLog() {
				ParsePartitionParameter();
				Prepare();
				ScanLog();
				Process();
				CleanUp();
			}

			virtual size_t ParseChopName(const std::string &chop_name) = 0;
			virtual void Prepare() = 0;
			virtual void ScanLog() = 0;
			virtual void Process() = 0;
			virtual void CleanUp() = 0;

		private:
			void ParsePartitionParameter() {
				for (size_t i = 0; i < kMaxChopNum; ++i) {
					partitions_[i] = 1;
				}
				std::ifstream infile(partition_filename_);
				assert(infile.good() == true);
				std::string line;
				while (std::getline(infile, line)) {
					std::string chop_name = line.substr(0, line.find("="));
					std::string num_str = line.substr(line.find("=") + 1);
					partitions_[ParseChopName(chop_name)] = atoi(num_str.c_str());
				}
				infile.close();
			}

		private:
			BaseParallelReplayer(const BaseParallelReplayer &);
			BaseParallelReplayer& operator=(const BaseParallelReplayer &);

		protected:
			TransactionManager transaction_manager_;
			ScheduleGraph schedule_graph_;
			std::hash<std::string> hash_function_;
			BoostTransactionPool transaction_pool_;
			std::string partition_filename_;
			size_t partitions_[kMaxChopNum];
		};
	}
}

#endif
