#pragma once
#ifndef __CAVALIA_STORAGE_ENGINE_SCHEDULE_GRAPH_H__
#define __CAVALIA_STORAGE_ENGINE_SCHEDULE_GRAPH_H__

#include <cassert>
#include <unordered_map>

namespace Cavalia{
	namespace StorageEngine{
		struct ScheduleNode{
			ScheduleNode() : total_parent_num_(0), partition_num_(1){}
			size_t total_parent_num_;
			std::vector<size_t> child_ids_;
			size_t partition_num_;
		};

		class ScheduleGraph{
		public:
			ScheduleGraph(){}
			~ScheduleGraph(){
				delete[] schedule_nodes_;
				schedule_nodes_ = NULL;
			}

			void SetNodeCount(const size_t &node_num){
				schedule_nodes_ = new ScheduleNode[node_num];
				node_num_ = node_num;
			}

			void SetPartition(const size_t &node_id, const size_t &partition_num = 1){
				schedule_nodes_[node_id].partition_num_ = partition_num;
			}

			void AddDependence(const size_t &src_id, const size_t &dst_id){
				schedule_nodes_[dst_id].total_parent_num_ += 1;
				schedule_nodes_[src_id].child_ids_.push_back(dst_id);
			}

		private:
			ScheduleGraph(const ScheduleGraph &);
			ScheduleGraph& operator=(const ScheduleGraph &);

		public:
			ScheduleNode *schedule_nodes_;
			size_t node_num_;
		};
	}
}

#endif
