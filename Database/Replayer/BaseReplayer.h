#pragma once
#ifndef __CAVALIA_DATABASE_BASE_REPLAYER_H__
#define __CAVALIA_DATABASE_BASE_REPLAYER_H__

#include <boost/thread.hpp>
#include <TimeMeasurer.h>
#include <cstdio>
#include "../Storage/BaseStorageManager.h"
#include "../Meta/MetaConstants.h"

namespace Cavalia{
	namespace Database{

		struct CommandLogEntry{
			CommandLogEntry(){}
			CommandLogEntry(const uint64_t &timestamp, TxnParam *param){
				timestamp_ = timestamp;
				param_ = param;
			}

			uint64_t timestamp_;
			TxnParam *param_;
		};

		typedef std::vector<CommandLogEntry*> CommandLogEntries;

		struct ValueLogElement{
			uint8_t type_;
			uint8_t table_id_;
			uint8_t data_size_;
			char *data_ptr_;
		};

		struct ValueLogEntry{
			ValueLogEntry(){
				element_count_ = 0;
				timestamp_ = 0;
			}

			ValueLogElement* NewValueLogElement(){
				assert(element_count_ < kMaxAccessNum);
				ValueLogElement *ret = &elements_[element_count_];
				++element_count_;
				return ret;
			}

			void Clear(){
				element_count_ = 0;
			}

			ValueLogElement elements_[kMaxAccessNum];
			size_t element_count_;
			uint64_t timestamp_;
		};

		typedef std::vector<ValueLogEntry*> ValueLogEntries;

		class BaseReplayer{
		public:
			BaseReplayer(const std::string &dir_name, BaseStorageManager *const storage_manager, const size_t &thread_count, bool is_vl) : dir_name_(dir_name), storage_manager_(storage_manager), thread_count_(thread_count){
				infiles_ = new FILE*[thread_count_];
				// for value logging
				if (is_vl == true){
					for (size_t i = 0; i < thread_count_; ++i){
						infiles_[i] = fopen((dir_name_ + "/valuelog" + std::to_string(i)).c_str(), "rb");
						assert(infiles_[i] != NULL);
					}
				}
				// for command logging
				else{
					for (size_t i = 0; i < thread_count_; ++i){
						infiles_[i] = fopen((dir_name_ + "/commandlog" + std::to_string(i)).c_str(), "rb");
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
