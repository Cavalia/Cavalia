#pragma once
#ifndef __CAVALIA_STORAGE_ENGINE_BOOST_TRANSACTION_POOL_H__
#define __CAVALIA_STORAGE_ENGINE_BOOST_TRANSACTION_POOL_H__

#include <boost/asio/io_service.hpp>
#include <boost/bind.hpp>
#include <boost/thread/thread.hpp>
#include <thread>

namespace Cavalia{
	namespace StorageEngine{
		class BoostTransactionPool{
		public:
			BoostTransactionPool() : pool_size_(0), work_(io_service_){}
			BoostTransactionPool(const size_t &pool_size) : pool_size_(pool_size), work_(io_service_){}

			~BoostTransactionPool(){
				io_service_.stop();
				thread_pool_.join_all();
			}

			void SetPoolSize(const size_t &pool_size){
				pool_size_ = pool_size;
			}

			void InitiatePool(){
				assert(pool_size_ != 0);
				for (size_t i = 0; i < pool_size_; ++i){
					thread_pool_.create_thread(boost::bind(&boost::asio::io_service::run, &io_service_));
				}
			}

		private:
			BoostTransactionPool(const BoostTransactionPool &);
			BoostTransactionPool& operator=(const BoostTransactionPool &);

		public:
			boost::asio::io_service io_service_;

		private:
			size_t pool_size_;
			boost::thread_group thread_pool_;
			boost::asio::io_service::work work_;
		};
	}
}

#endif
