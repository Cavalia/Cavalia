#pragma once
#ifndef __CAVALIA_STORAGE_ENGINE_IO_REDIRECTOR_H__
#define __CAVALIA_STORAGE_ENGINE_IO_REDIRECTOR_H__

#include <EventTuple.h>
#include <thread>
#include <vector>
#include "MetaTypes.h"

namespace Cavalia {
	namespace StorageEngine {
		class IORedirector {
		public:
			IORedirector(const size_t &thread_count) : thread_count_(thread_count), curr_thread_id_(0){
				input_batches_ = new std::vector<TupleBatch*>[thread_count];
			}
			~IORedirector() {
				delete[] input_batches_;
				input_batches_ = NULL;
			}

			std::vector<TupleBatch*> *GetParameterBatches() {
				return input_batches_;
			}
			std::vector<TupleBatch*> *GetParameterBatches(const size_t &thread_id) {
				return &(input_batches_[thread_id]);
			}
			void PushParameterBatch(TupleBatch *tuples) {
				input_batches_[curr_thread_id_].push_back(tuples);
				curr_thread_id_ = (curr_thread_id_ + 1) % thread_count_;
			}

		private:
			IORedirector(const IORedirector &);
			IORedirector& operator=(const IORedirector &);

		protected:
			std::vector<TupleBatch*> *input_batches_;
			size_t thread_count_;
			size_t curr_thread_id_;
		};
	}
}

#endif
