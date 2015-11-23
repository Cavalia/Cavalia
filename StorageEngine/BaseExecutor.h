#pragma once
#ifndef __CAVALIA_STORAGE_ENGINE_BASE_EXECUTOR_H__
#define __CAVALIA_STORAGE_ENGINE_BASE_EXECUTOR_H__

#include <TimeMeasurer.h>
#include "BaseStorageManager.h"
#include "BaseLogger.h"
#include "IORedirector.h"
#include "StoredProcedure.h"

namespace Cavalia{
	namespace StorageEngine{
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
