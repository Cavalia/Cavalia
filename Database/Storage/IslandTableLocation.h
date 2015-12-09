#pragma once
#ifndef __CAVALIA_DATABASE_EXECUTOR_ISLAND_TABLE_LOCATION_H__
#define __CAVALIA_DATABASE_EXECUTOR_ISLAND_TABLE_LOCATION_H__

#include <vector>

namespace Cavalia{
	namespace Database{
		struct IslandTableLocation{
			size_t GetPartitionCount() const {
				return node_ids_.size();
			}

			size_t Partition2Node(const size_t &partition_id) const {
				return node_ids_.at(partition_id);
			}

			void AddPartition(const size_t &node_id) {
				node_ids_.push_back(node_id);
			}

			void SetPartitionId(const size_t &node_id){
				partition_id_ = node_id;
			}

			size_t GetPartitionId() const {
				return partition_id_;
			}

		private:
			std::vector<size_t> node_ids_;
			size_t partition_id_;
		};
	}
}

#endif
