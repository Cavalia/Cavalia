#pragma once
#ifndef __CAVALIA_DATABASE_HSTORE_CONTENT_H__
#define __CAVALIA_DATABASE_HSTORE_CONTENT_H__

#include <set>
#include <AllocatorHelper.h>
#include <boost/thread/mutex.hpp>
#include "../Meta/MetaTypes.h"

namespace Cavalia{
	namespace Database{
		class HStoreContent{
		public:
			HStoreContent(boost::detail::spinlock **spinlocks, const size_t &thread_count){
				spinlocks_ = new boost::detail::spinlock*[thread_count];
				memcpy(spinlocks_, spinlocks, sizeof(void*)*thread_count);
			}
			~HStoreContent(){
				delete[] spinlocks_;
				spinlocks_ = NULL;
			}

			void LockPartition(const size_t &part_id){
				spinlocks_[part_id]->lock();
			}
			
			void UnlockPartition(const size_t &part_id){
				spinlocks_[part_id]->unlock();
			}

			void LockPartitions(const std::set<size_t> &part_ids){
				for (auto &part_id : part_ids){
					spinlocks_[part_id]->lock();
				}
			}

			void UnlockPartitions(const std::set<size_t> &part_ids){
				for (auto &part_id : part_ids){
					spinlocks_[part_id]->unlock();
				}
			}

		private:
			boost::detail::spinlock **spinlocks_;
		};
	}
}

#endif