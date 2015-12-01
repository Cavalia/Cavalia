#pragma once
#ifndef __CAVALIA_DATABASE_BASE_REPLAYER_H__
#define __CAVALIA_DATABASE_BASE_REPLAYER_H__

#include <TimeMeasurer.h>
#include <cstdio>
#include "../Storage/BaseStorageManager.h"

namespace Cavalia{
	namespace Database{
		class BaseReplayer{
		public:
			BaseReplayer(const std::string &dir_name, BaseStorageManager *const storage_manager, const size_t &thread_count, bool is_vl) : dir_name_(dir_name), storage_manager_(storage_manager), thread_count_(thread_count){
				infiles_ = new FILE*[thread_count_];
				// is value logging
				if (is_vl == true){
					for (size_t i = 0; i < thread_count_; ++i){
						infiles_[i] = fopen((dir_name_ + "/vl" + std::to_string(i)).c_str(), "rb");
						assert(infiles_[i] != NULL);
					}
				}
				// is command logging
				else{
					for (size_t i = 0; i < thread_count_; ++i){
						infiles_[i] = fopen((dir_name_ + "/cl" + std::to_string(i)).c_str(), "rb");
						assert(infiles_[i] != NULL);
					}
				}
			}
			virtual ~BaseReplayer(){
				for (size_t i = 0; i < thread_count_; ++i){
					int ret = fclose(infiles_[i]);
					assert(ret == 0);
				}
				delete[] infiles_;
				infiles_ = NULL;
			}

			virtual void Start() = 0;

		private:
			BaseReplayer(const BaseReplayer &);
			BaseReplayer& operator=(const BaseReplayer &);

		protected:
			std::string dir_name_;
			BaseStorageManager *const storage_manager_;
			size_t thread_count_;
			FILE **infiles_;
		};
	}
}

#endif
