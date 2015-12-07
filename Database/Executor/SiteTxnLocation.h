#pragma once
#ifndef __CAVALIA_DATABASE_EXECUTOR_SITE_TXN_LOCATION_H__
#define __CAVALIA_DATABASE_EXECUTOR_SITE_TXN_LOCATION_H__

#include <vector>

namespace Cavalia{
	namespace Database{
		struct SiteTxnLocation{
			void SetPartitionCount(const size_t &node_count){
				node_count_ = node_count;
			}

			size_t GetPartitionCount() const {
				return node_count_;
			}

			size_t GetThreadCount() const{
				return core_ids_.size();
			}

			size_t Thread2Core(const size_t &thread_id) const {
				return core_ids_.at(thread_id);
			}

			void AddThread(const size_t &core_id) {
				core_ids_.push_back(core_id);
			}

		private:
			size_t node_count_;
			std::vector<size_t> core_ids_;
		};
	}
}

#endif
