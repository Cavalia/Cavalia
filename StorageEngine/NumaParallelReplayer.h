#pragma once
#ifndef __CAVALIA_STORAGE_ENGINE_NUMA_PARALLEL_REPLAYER_H__
#define __CAVALIA_STORAGE_ENGINE_NUMA_PARALLEL_REPLAYER_H__

#include <boost/lockfree/queue.hpp>
#include <functional>
#include <vector>
#include "BaseReplayer.h"
#include "ScheduleGraph.h"
#include "DynamicScheduler.h"

namespace Cavalia{
	namespace StorageEngine{
		class NumaParallelReplayer : public BaseReplayer{
		public:
			NumaParallelReplayer(const std::string &filename, BaseStorageManager *const storage_manager, const size_t &thread_count, const std::string &partition_filename) : BaseReplayer(filename, storage_manager, thread_count){
				std::ifstream infile(partition_filename);
				std::string line;
				while (std::getline(infile, line)){
					std::cout << line << std::endl;
					std::string num_str = line.substr(line.find("=") + 1);
					std::cout << num_str << std::endl;
					partitions_.push_back(atoi(num_str.c_str()));
				}
				for (auto &entry : partitions_){
					std::cout << entry << std::endl;
				}
				getchar();
			}

			virtual ~NumaParallelReplayer(){}

			virtual void ProcessLog() {
				Prepare();
				ScanLog();
				Process();
				CleanUp();
			}

			virtual void Prepare() = 0;
			virtual void ScanLog() = 0;
			virtual void Process() = 0;
			virtual void CleanUp() = 0;

		private:
			NumaParallelReplayer(const NumaParallelReplayer &);
			NumaParallelReplayer& operator=(const NumaParallelReplayer &);

		protected:
			std::vector<size_t> partitions_;
			ScheduleGraph schedule_graph_;
			DynamicScheduler scheduler_;
			std::hash<std::string> hash_function_;
		};
	}
}

#endif
