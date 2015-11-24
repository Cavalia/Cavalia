#pragma once
#ifndef __CAVALIA_STORAGE_ENGINE_NUMA_TRANSACTION_POOL_H__
#define __CAVALIA_STORAGE_ENGINE_NUMA_TRANSACTION_POOL_H__

#include <cassert>
#include <pthread.h>

namespace Cavalia{
	namespace StorageEngine{
		class NumaTransactionPool{
		public:
			NumaTransactionPool() : pool_size_(0){}
			NumaTransactionPool(const size_t &pool_size) : pool_size_(pool_size){}

			~NumaTransactionPool(){
				for (size_t i = 0; i < this->pool_size_; ++i){
					pthread_join(threads_[i], NULL);
				}
				delete[] threads_;
				delete[] masks_;
				delete[] attrs_;
			}

			void SetPool(const size_t &socket_id, const size_t &pool_size){
				socket_id_ = socket_id;
				pool_size_ = pool_size;
			}

			void InitiatePool(){
				assert(pool_size_ != 0);
				threads_ = new pthread_t[this->pool_size_];
				masks_ = new cpu_set_t[this->pool_size_];
				attrs_ = new pthread_attr_t[this->pool_size_];
				for (size_t i = 0; i < pool_size_; ++i){
					size_t core_id = (socket_id_ / 2) * 16 + i;
					CPU_ZERO(&masks_[i]);
					CPU_SET(core_id, &masks_[i]);
					pthread_attr_init(&attrs_[i]);
					int res = pthread_attr_setaffinity_np(&attrs_[i], sizeof(cpu_set_t), &masks_[i]);
					assert(res == 0);

				}
			}

		private:
			NumaTransactionPool(const NumaTransactionPool &);
			NumaTransactionPool& operator=(const NumaTransactionPool &);

		private:
			size_t socket_id_;
			size_t pool_size_;
			pthread_t *threads_;
			cpu_set_t *masks_;
			pthread_attr_t *attrs_;
		};
	}
}

#endif
