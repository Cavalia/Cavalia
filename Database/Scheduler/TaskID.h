#pragma once
#ifndef __CAVALIA_DATABASE_TASK_ID_H__
#define __CAVALIA_DATABASE_TASK_ID_H__

namespace Cavalia{
	namespace Database{
		struct TaskID{
			TaskID(){
				slice_id_ = 0;
				layer_id_ = 0;
				partition_id_ = 0;
			}
			TaskID(const size_t &slice_id, const size_t &layer_id){
				slice_id_ = slice_id;
				layer_id_ = layer_id;
				partition_id_ = 0;
			}
			TaskID(const size_t &slice_id, const size_t &layer_id, const size_t &partition_id){
				slice_id_ = slice_id;
				layer_id_ = layer_id;
				partition_id_ = partition_id;
			}
			size_t slice_id_;
			size_t layer_id_;
			size_t partition_id_;
		};
	}
}

#endif
