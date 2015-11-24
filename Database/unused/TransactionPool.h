#pragma once
#ifndef __CAVALIA_STORAGE_ENGINE_THREAD_POOL_H__
#define __CAVALIA_STORAGE_ENGINE_THREAD_POOL_H__

#include <thread>
#include <vector>
#include <functional>
#include <future>
#include <boost/lockfree/queue.hpp>

namespace Cavalia{
	namespace StorageEngine{
		class TransactionPool{
		public:
			TransactionPool(const size_t &pool_size) : pool_size_(pool_size), task_queue_(10), is_end_(false){
				InitiatePool();
			}
			~TransactionPool(){
				is_end_ = true;
				for (size_t i = 0; i < threads_.size(); ++i){
					if (threads_.at(i).joinable())
						threads_.at(i).join();
				}
			}

		public:
			const size_t& GetPoolSize() const { return pool_size_; }

			void InitiatePool(){
				for (size_t i = 0; i < pool_size_; ++i){
					threads_.push_back(std::thread(&TransactionPool::workerThread, this));
				}
			}

			void ResetPoolSize(const size_t &pool_size){
				pool_size_ = pool_size;
			}

			template<typename FunctionType, typename... ParamTypes>
			// return future with type of decltype(func(param)).
			auto SubmitTask(FunctionType&& func, const ParamTypes&&... param)->std::future<decltype(func(param...))>{
				// get return type.
				typedef typename std::result_of<FunctionType && (ParamTypes&&...)>::type ReturnType;
				// another approach to get type.
				//typedef decltype(func(param)) ReturnType;
				// wrap function with packaged_task.
				std::packaged_task<ReturnType()> *packaged_task = new std::packaged_task<ReturnType()>(std::bind(func, param...));
				// get future.
				auto future = packaged_task->get_future();
				// call packaged_task in lambda function.
				std::function<void()> *task = new std::function<void()>([packaged_task](){(*packaged_task)(); });
				// push the lambda function into task queue.
				while (!task_queue_.push(task));
				return future;
			}

			template<typename FunctionType>
			// return future with type of decltype(func(param)).
			auto SubmitTask(FunctionType&& func)->std::future<decltype(func())>{
				// get return type.
				typedef typename std::result_of<FunctionType && ()>::type ReturnType;
				// another approach to get type.
				//typedef decltype(func(param)) ReturnType;
				// wrap function with packaged_task.
				std::packaged_task<ReturnType()> *packaged_task = new std::packaged_task<ReturnType()>(std::bind(func));
				// get future.
				auto future = packaged_task->get_future();
				// call packaged_task in lambda function.
				std::function<void()> *task = new std::function<void()>([packaged_task](){(*packaged_task)(); });
				// push the lambda function into task queue.
				while (!task_queue_.push(task));
				return future;
			}

			void workerThread(){
				while (!is_end_){
					std::function<void()> *task;
					if (task_queue_.pop(task)){
						(*task)();
					}
				}
			}
			// every time interval of tim, check whether it is finished
			bool IsFinished(int tim){
				while (!task_queue_.empty()){
					std::this_thread::sleep_for(std::chrono::seconds(tim));
				}
				return true;
			}

		private:
			TransactionPool(const TransactionPool &);
			TransactionPool& operator=(const TransactionPool &);

		private:
			volatile bool is_end_;
			size_t pool_size_;
			std::vector<std::thread> threads_;
			boost::lockfree::queue<std::function<void()> *> task_queue_;
		};
	}
}

#endif
