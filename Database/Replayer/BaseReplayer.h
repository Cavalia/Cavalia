#pragma once
#ifndef __CAVALIA_DATABASE_BASE_REPLAYER_H__
#define __CAVALIA_DATABASE_BASE_REPLAYER_H__

#include <boost/thread.hpp>
#if defined(COMPRESSION)
#include <lz4frame.h>
#endif
#include <TimeMeasurer.h>
#include <AllocatorHelper.h>
#include "../Storage/BaseStorageManager.h"
#include "../Meta/MetaTypes.h"
#include "LogEntry.h"

namespace Cavalia{
	namespace Database{

		class BaseReplayer{
		public:
			BaseReplayer(const std::string &dir_name, BaseStorageManager *const storage_manager, const size_t &thread_count, bool is_vl) : dir_name_(dir_name), storage_manager_(storage_manager), thread_count_(thread_count){
				infiles_ = new FILE*[thread_count_];
				// for value logging
				if (is_vl == true){
					for (size_t i = 0; i < thread_count_; ++i){
#if defined(COMPRESSION)
						infiles_[i] = fopen((dir_name_ + "/valuelog-compress-" + std::to_string(i)).c_str(), "rb");
#else
						infiles_[i] = fopen((dir_name_ + "/valuelog-" + std::to_string(i)).c_str(), "rb");
#endif
						assert(infiles_[i] != NULL);
					}
				}
				// for command logging
				else{
					for (size_t i = 0; i < thread_count_; ++i){
#if defined(COMPRESSION)
						infiles_[i] = fopen((dir_name_ + "/commandlog-compress-" + std::to_string(i)).c_str(), "rb");
#else
						infiles_[i] = fopen((dir_name_ + "/commandlog-" + std::to_string(i)).c_str(), "rb");
#endif
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
