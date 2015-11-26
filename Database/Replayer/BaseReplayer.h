#pragma once
#ifndef __CAVALIA_DATABASE_BASE_REPLAYER_H__
#define __CAVALIA_DATABASE_BASE_REPLAYER_H__

#include <TimeMeasurer.h>
#include <unordered_map>
#include <fstream>
#include <thread>
#include "../Transaction/StoredProcedure.h"
#include "../Storage/BaseStorageManager.h"

namespace Cavalia{
	namespace Database{
		class BaseReplayer{
		public:
			BaseReplayer(const std::string &dir_name, BaseStorageManager *const storage_manager, const size_t &thread_count, bool is_vl) : dir_name_(dir_name), storage_manager_(storage_manager), thread_count_(thread_count){
				infiles_ = new std::ifstream[thread_count_];
				// is value logging
				if (is_vl == true){
					for (size_t i = 0; i < thread_count_; ++i){
						infiles_[i].open(dir_name_ + "/vl" + std::to_string(i), std::ifstream::binary);
						assert(infiles_[i].good() == true);
					}
				}
				// is command logging
				else{
					for (size_t i = 0; i < thread_count_; ++i){
						infiles_[i].open(dir_name_ + "/cl" + std::to_string(i), std::ifstream::binary);
						assert(infiles_[i].good() == true);
					}
				}
			}
			virtual ~BaseReplayer(){
				for (size_t i = 0; i < thread_count_; ++i){
					infiles_[i].close();
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
			std::ifstream *infiles_;
		};
	}
}

#endif
