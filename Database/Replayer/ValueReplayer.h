#pragma once
#ifndef __CAVALIA_DATABASE_VALUE_REPLAYER_H__
#define __CAVALIA_DATABASE_VALUE_REPLAYER_H__

#include "../Storage/BaseStorageManager.h"
#include "BaseReplayer.h"

namespace Cavalia{
	namespace Database{
		class ValueReplayer : public BaseReplayer{
		public:
			ValueReplayer(const std::string &filename, BaseStorageManager *const storage_manager, const size_t &thread_count) : BaseReplayer(filename, storage_manager, thread_count, true){}
			virtual ~ValueReplayer(){}

			virtual void Start(){
				boost::thread_group reloaders;
				for (size_t i = 0; i < thread_count_; ++i){
					reloaders.create_thread(boost::bind(&ValueReplayer::ReloadLog, this, i));
				}
				reloaders.join_all();
			}

			void ReloadLog(const size_t &thread_id){
				
			}

			void ProcessLog(const size_t &thread_id){
				
			}

		private:
			ValueReplayer(const ValueReplayer &);
			ValueReplayer& operator=(const ValueReplayer &);
		};
	}
}

#endif
