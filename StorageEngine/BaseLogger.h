#pragma once
#ifndef __CAVALIA_STORAGE_ENGINE_BASE_LOGGER_H__
#define __CAVALIA_STORAGE_ENGINE_BASE_LOGGER_H__

#include <vector>
#include <cstdint>
#include <EventTuple.h>
#include "MetaTypes.h"

namespace Cavalia{
	namespace StorageEngine{
		class BaseLogger{
		public:
			BaseLogger(const size_t &thread_count) : thread_count_(thread_count){}
			~BaseLogger(){}

			void CommitTransaction(const size_t &thread_id, const uint64_t &commit_ts, CharArray *log_str) {}

		private:
			BaseLogger(const BaseLogger &);
			BaseLogger& operator=(const BaseLogger &);

		private:
			size_t thread_count_;
		};
	}
}

#endif
