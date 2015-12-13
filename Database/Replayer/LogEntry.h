#pragma once
#ifndef __CAVALIA_DATABASE_LOG_ENTRY_H__
#define __CAVALIA_DATABASE_LOG_ENTRY_H__

#include <cstdint>
#include "../Transaction/TxnParam.h"

namespace Cavalia{
	namespace Database{

		struct BaseLogEntry{
			BaseLogEntry() : timestamp_(0){}
			BaseLogEntry(const uint64_t &timestamp) : timestamp_(timestamp){}
			uint64_t timestamp_;
		};

		typedef std::vector<BaseLogEntry*> LogEntries;

		struct CommandLogEntry : public BaseLogEntry{
			CommandLogEntry(){}
			CommandLogEntry(const uint64_t &timestamp, TxnParam *param) : BaseLogEntry(timestamp), param_(param){}

			TxnParam *param_;
		};

		typedef std::vector<CommandLogEntry*> CommandLogEntries;

		struct ValueLogElement{
			uint8_t type_;
			uint8_t table_id_;
			uint8_t data_size_;
			char *data_ptr_;
		};

		struct ValueLogEntry : public BaseLogEntry{
			ValueLogEntry() : element_count_(0){}

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
		};

		typedef std::vector<ValueLogEntry*> ValueLogEntries;
	}
}

#endif
