#pragma once
#ifndef __CAVALIA_DATABASE_BASE_LOGGER_H__
#define __CAVALIA_DATABASE_BASE_LOGGER_H__

#include <string>
#include <cstdio>
#if defined(__linux__)
#include <unistd.h>
#endif
#include <NumaHelper.h>
#include "../Meta/MetaConstants.h"

namespace Cavalia{
	namespace Database{
		class BaseLogger{
		public:
			BaseLogger(const std::string &dir_name, const size_t &thread_count, bool is_vl) : dir_name_(dir_name), thread_count_(thread_count){
				outfiles_ = new FILE*[thread_count_];
				// is value logging
				if (is_vl == true){
					for (size_t i = 0; i < thread_count_; ++i){
						outfiles_[i] = fopen((dir_name_ + "/valuelog" + std::to_string(i)).c_str(), "wb");
						assert(outfiles_[i] != NULL);
					}
				}
				// is command logging
				else{
					for (size_t i = 0; i < thread_count_; ++i){
						outfiles_[i] = fopen((dir_name_ + "/commandlog" + std::to_string(i)).c_str(), "wb");
						assert(outfiles_[i] != NULL);
					}
				}
			}
			virtual ~BaseLogger(){
				for (size_t i = 0; i < thread_count_; ++i){
					int ret;
					ret = fflush(outfiles_[i]);
					assert(ret == 0);
#if defined(__linux__)
					ret = fsync(fileno(outfiles_[i]));
					assert(ret == 0);
#endif
					ret = fclose(outfiles_[i]);
					assert(ret == 0);
				}
				delete[] outfiles_;
				outfiles_ = NULL;
			}

		private:
			BaseLogger(const BaseLogger &);
			BaseLogger& operator=(const BaseLogger &);

		protected:
			std::string dir_name_;
			size_t thread_count_;
			FILE **outfiles_;
		};
	}
}

#endif
