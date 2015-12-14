#pragma once
#ifndef __CAVALIA_DATABASE_BASE_EXECUTOR_H__
#define __CAVALIA_DATABASE_BASE_EXECUTOR_H__

#include <TimeMeasurer.h>
#include <AllocatorHelper.h>
#include "../Redirector/IORedirector.h"
#include "../Transaction/StoredProcedure.h"
#include "../Storage/BaseStorageManager.h"
#include "../Logger/BaseLogger.h"

namespace Cavalia{
	namespace Database{
		class BaseExecutor{
		public:
			BaseExecutor(IORedirector *const redirector, BaseLogger *const logger, const size_t &thread_count) : redirector_ptr_(redirector), logger_(logger), thread_count_(thread_count){}
			virtual ~BaseExecutor(){}

			virtual void Start() = 0;

		private:
			BaseExecutor(const BaseExecutor &);
			BaseExecutor& operator=(const BaseExecutor &);

		protected:
			IORedirector *const redirector_ptr_;
			BaseLogger *const logger_;
			size_t thread_count_;
		};
	}
}

#endif
