#pragma once
#ifndef __CAVALIA_STORAGE_ENGINE_DYNAMIC_SCHEDULER_H__
#define __CAVALIA_STORAGE_ENGINE_DYNAMIC_SCHEDULER_H__

#include <cassert>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "ScheduleGraph.h"

namespace Cavalia{
	namespace StorageEngine{

		class DynamicScheduler{
		public:
			DynamicScheduler(){}
			~DynamicScheduler(){
				delete[] dependencies_;
				dependencies_ = NULL;
				delete[] partitions_;
				partitions_ = NULL;
				delete[] local_mutex_;
				local_mutex_ = NULL;
			}

			void RegisterGraph(ScheduleGraph *graph) {
				graph_ = graph;
				total_task_num_ = graph->node_num_;
				submitted_task_num_ = 0;
				completed_task_num_ = 0;
				dependencies_ = new size_t[total_task_num_];
				memset(dependencies_, 0, sizeof(size_t)*total_task_num_);
				partitions_ = new size_t[total_task_num_];
				memset(partitions_, 0, sizeof(size_t)*total_task_num_);
				local_mutex_ = new std::mutex[total_task_num_];
				for (size_t i = 0; i < total_task_num_; ++i) {
					queueing_tasks_.insert(i);
				}
			}

			void Clear(){
				submitted_task_num_ = 0;
				completed_task_num_ = 0;
				memset(dependencies_, 0, sizeof(size_t)*total_task_num_);
				memset(partitions_, 0, sizeof(size_t)*total_task_num_);
				for (size_t i = 0; i < total_task_num_; ++i) {
					queueing_tasks_.insert(i);
				}
			}

			bool CompleteTask(const size_t &task_id){
				std::lock_guard<std::mutex> local_lock(local_mutex_[task_id]);
				partitions_[task_id] += 1;
				if (partitions_[task_id] == graph_->schedule_nodes_[task_id].partition_num_){
					std::lock_guard<std::mutex> global_lock(global_mutex_);
					completed_task_num_ += 1;
					for (auto &child_id : graph_->schedule_nodes_[task_id].child_ids_){
						dependencies_[child_id] += 1;
					}
					return true;
				}
				return false;
			}

			bool CompleteTaskLocal(const size_t &task_id){
				std::lock_guard<std::mutex> local_lock(local_mutex_[task_id]);
				partitions_[task_id] += 1;
				return partitions_[task_id] == graph_->schedule_nodes_[task_id].partition_num_;
			}

			void CompleteTaskGlobal(const size_t &task_id){
				std::lock_guard<std::mutex> global_lock(global_mutex_);
				completed_task_num_ += 1;
				for (auto &child_id : graph_->schedule_nodes_[task_id].child_ids_){
					dependencies_[child_id] += 1;
				}
			}

			void SubmitTask(const size_t &task_id){
				submitted_task_num_ += 1;
				queueing_tasks_.erase(task_id);
			}

			bool IsSubmittable(const size_t &task_id) const {
				return dependencies_[task_id] == graph_->schedule_nodes_[task_id].total_parent_num_;
			}

			bool IsAllSubmitted() const {
				return submitted_task_num_ >= total_task_num_;
			}

			bool IsAllCompleted() const {
				return completed_task_num_ >= total_task_num_;
			}

		private:
			DynamicScheduler(const DynamicScheduler &);
			DynamicScheduler& operator=(const DynamicScheduler &);

		public:
			// static
			ScheduleGraph *graph_;
			size_t total_task_num_;

			// dynamic
			size_t *dependencies_;
			size_t *partitions_;
			size_t submitted_task_num_;
			size_t completed_task_num_;
			std::mutex *local_mutex_;
			std::mutex global_mutex_;
			std::unordered_set<size_t> queueing_tasks_;
		};
	}
}

#endif
