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
			
			}

		private:
			ValueReplayer(const ValueReplayer &);
			ValueReplayer& operator=(const ValueReplayer &);
		};
	}
}

#endif
