#pragma once
#ifndef __CAVALIA_DATABASE_BASE_LOGGER_H__
#define __CAVALIA_DATABASE_BASE_LOGGER_H__

#include <vector>
#include <cstdint>
#include <EventTuple.h>
#include "../Meta/MetaTypes.h"

namespace Cavalia{
	namespace Database{
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
