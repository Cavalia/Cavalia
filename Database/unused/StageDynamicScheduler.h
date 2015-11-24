#pragma once
#ifndef __CAVALIA_STORAGE_ENGINE_STAGE_DYNAMIC_SCHEDULER_H__
#define __CAVALIA_STORAGE_ENGINE_STAGE_DYNAMIC_SCHEDULER_H__

#include <cassert>
#include <atomic>
#include <boost/thread/mutex.hpp>

#include "TaskID.h"
#include "ScheduleGraph.h"
#include "MetaTypes.h"

namespace Cavalia{
	namespace StorageEngine{
		class StageDynamicScheduler{
		public:
			StageDynamicScheduler(){}
			~StageDynamicScheduler(){}

			void RegisterGraph(ScheduleGraph *graph, const size_t &layer_count){
				graph_ = graph;
				node_num_ = graph->node_num_;
				layer_num_ = layer_count;
				max_task_num_ = node_num_*layer_num_;

				submitted_task_num_ = 0;
				completed_task_num_ = 0;

				layer_dependencies_ = new std::atomic<size_t>[layer_num_];
				dependencies_ = new std::atomic<size_t>[max_task_num_];
				min_submittable_layer_ = 0;
				max_submittable_layer_ = 1;

				submitted_task_locks_ = new boost::detail::spinlock[max_task_num_];
				submitted_task_counts_ = new size_t[max_task_num_];
				submitted_task_flags_ = new bool[max_task_num_];

				submitted_layer_counts_ = new std::atomic<size_t>[layer_num_];

				completed_task_counts_ = new std::atomic<size_t>[max_task_num_];

				for (size_t i = 0; i < max_task_num_; ++i){
					dependencies_[i] = 0;

					memset(&(submitted_task_locks_[i]), 0, sizeof(boost::detail::spinlock));
					submitted_task_counts_[i] = 0;
					submitted_task_flags_[i] = false;

					completed_task_counts_[i] = 0;
				}

				for (size_t i = 0; i < layer_num_; ++i){
					submitted_layer_counts_[i] = 0;
					layer_dependencies_[i] = 0;
				}
				layer_dependencies_[0] = node_num_;
			}

			TaskID ParseTaskCode(const size_t &task_code){
				// return <node_id, layer_id>
				return TaskID(task_code % node_num_, task_code / node_num_);
			}

			bool CompleteTask(const TaskID &task_id){
				size_t task_code = task_id.layer_id_ * node_num_ + task_id.slice_id_;
				size_t last_value = completed_task_counts_[task_code].fetch_add(1, std::memory_order_relaxed);
				if (last_value == graph_->schedule_nodes_[task_id.slice_id_].partition_num_ - 1){
					completed_task_num_ += 1;
					for (auto &child_id : graph_->schedule_nodes_[task_id.slice_id_].child_ids_){
						dependencies_[task_id.layer_id_ * node_num_ + child_id] += 1;
					}
					if (task_id.layer_id_ < layer_num_ - 1){
						layer_dependencies_[task_id.layer_id_ + 1] += 1;
						if (task_id.layer_id_ + 1 > max_submittable_layer_){
							max_submittable_layer_ = task_id.layer_id_;
						}
					}
					return true;
				}
				return false;
			}

			bool CompleteTaskLocal(const TaskID &task_id){
				size_t task_code = task_id.layer_id_ * node_num_ + task_id.slice_id_;
				size_t last_value = completed_task_counts_[task_code].fetch_add(1, std::memory_order_relaxed);
				if (last_value == graph_->schedule_nodes_[task_id.slice_id_].partition_num_){
					return true;
				}
				return false;
			}

			void CompleteTaskGlobal(const TaskID &task_id){
				size_t task_code = task_id.layer_id_ * node_num_ + task_id.slice_id_;
				completed_task_num_ += 1;
				for (auto &child_id : graph_->schedule_nodes_[task_id.slice_id_].child_ids_){
					dependencies_[task_id.layer_id_ * node_num_ + child_id] += 1;
				}
				if (task_id.layer_id_ < layer_num_ - 1){
					layer_dependencies_[task_id.layer_id_ + 1] += 1;
					if (task_id.layer_id_ + 1 > max_submittable_layer_){
						max_submittable_layer_ = task_id.layer_id_;
					}
				}
			}

			TaskID AcquireTask(){
				while (1){
					for (size_t layer_id = min_submittable_layer_; layer_id < layer_num_; ++layer_id){
						for (size_t node_id = 0; node_id < node_num_; ++node_id){
							size_t task_code = layer_id * node_num_ + node_id;
							if (submitted_task_flags_[task_code] == true){
								continue;
							}
							if (layer_dependencies_[layer_id] == node_num_ && dependencies_[task_code] == graph_->schedule_nodes_[node_id].total_parent_num_){
								submitted_task_locks_[task_code].lock();
								if (submitted_task_counts_[task_code] != graph_->schedule_nodes_[node_id].partition_num_){
									size_t partition_id = submitted_task_counts_[task_code];
									++submitted_task_counts_[task_code];
									if (submitted_task_counts_[task_code] == graph_->schedule_nodes_[node_id].partition_num_){
										submitted_task_flags_[task_code] = true;
										submitted_task_num_ += 1;
										size_t last_value = submitted_layer_counts_[layer_id].fetch_add(1, std::memory_order_relaxed);
										if (last_value == node_num_){
											++min_submittable_layer_;
										}
									}
									submitted_task_locks_[task_code].unlock();
									return TaskID(node_id, layer_id, partition_id);
								}
								else{
									submitted_task_locks_[task_code].unlock();
								}
							}
						}
					}
					if (submitted_task_num_ == max_task_num_){
						return TaskID(SIZE_MAX, 0);
					}
				}
				return TaskID(SIZE_MAX, 0);
			}

			bool IsAllCompleted() const {
				return completed_task_num_ == max_task_num_;
			}

		private:
			StageDynamicScheduler(const StageDynamicScheduler &);
			StageDynamicScheduler& operator=(const StageDynamicScheduler &);

		public:
			// static
			ScheduleGraph *graph_;
			size_t node_num_;
			size_t layer_num_;
			size_t max_task_num_;

			// dynamic
			std::atomic<size_t> *layer_dependencies_;
			std::atomic<size_t> *dependencies_;
			size_t min_submittable_layer_;
			size_t max_submittable_layer_;

			boost::detail::spinlock *submitted_task_locks_;
			size_t *submitted_task_counts_;
			bool *submitted_task_flags_;

			std::atomic<size_t> *submitted_layer_counts_;
			std::atomic<size_t> submitted_task_num_;

			std::atomic<size_t> *completed_task_counts_;
			std::atomic<size_t> completed_task_num_;

		};
	}
}

#endif
