#pragma once
#ifndef __CAVALIA_DATABASE_BASE_LOGGER_H__
#define __CAVALIA_DATABASE_BASE_LOGGER_H__

#include <string>
#include <fstream>

namespace Cavalia{
	namespace Database{
		class BaseLogger{
		public:
			BaseLogger(const std::string &dir_name, const size_t &thread_count, bool is_vl) : dir_name_(dir_name), thread_count_(thread_count){
				outfiles_ = new std::ofstream[thread_count_];
				// is value logging
				if (is_vl == true){
					for (size_t i = 0; i < thread_count_; ++i){
						outfiles_[i].open(dir_name_ + "/vl" + std::to_string(i));
					}
				}
				// is command logging
				else{
					for (size_t i = 0; i < thread_count_; ++i){
						outfiles_[i].open(dir_name_ + "/cl" + std::to_string(i));
					}
				}
			}
			virtual ~BaseLogger(){
				for (size_t i = 0; i < thread_count_; ++i){
					outfiles_[i].flush();
					outfiles_[i].close();
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
			std::ofstream *outfiles_;
		};
	}
}

#endif
