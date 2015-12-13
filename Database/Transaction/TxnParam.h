#pragma once
#ifndef __CAVALIA_DATABASE_TXN_PARAM_H__
#define __CAVALIA_DATABASE_TXN_PARAM_H__

#include <cassert>
#include <cstdint>
#include <vector>
#include <CharArray.h>
#include "../Meta/MetaTypes.h"

namespace Cavalia{
	namespace Database{
		class TxnParam{
		public:
			TxnParam(){}
			virtual ~TxnParam(){}
			virtual uint64_t GetHashCode() const = 0;
			virtual void Serialize(CharArray& serial_str) const = 0;
			virtual void Serialize(char *buffer, size_t &buffer_size) const = 0;
			virtual void Deserialize(const CharArray& serial_str) = 0;

		public:
			size_t type_;
		};

		class ParamBatch {
		public:
			ParamBatch() {
				params_ = new TxnParam*[gParamBatchSize];
				param_count_ = 0;
				batch_size_ = gParamBatchSize;
			}
			ParamBatch(const size_t &batch_size) {
				params_ = new TxnParam*[batch_size];
				param_count_ = 0;
				batch_size_ = batch_size;
			}
			~ParamBatch() {
				delete[] params_;
				params_ = NULL;
			}

			void push_back(TxnParam *tuple) {
				assert(param_count_ < batch_size_);
				params_[param_count_] = tuple;
				++param_count_;
			}

			size_t size() const {
				return param_count_;
			}

			TxnParam* get(const size_t idx) const {
				return params_[idx];
			}

		private:
			TxnParam **params_;
			size_t param_count_;
			size_t batch_size_;
		};

		struct ParamPtrWrapper{
			size_t part_id_;
			TxnParam *param_;
		};

		class ParamBatchWrapper {
		public:
			ParamBatchWrapper() {
				params_ = new ParamPtrWrapper[gParamBatchSize];
				param_count_ = 0;
				batch_size_ = gParamBatchSize;
			}
			ParamBatchWrapper(const size_t &batch_size) {
				params_ = new ParamPtrWrapper[batch_size];
				param_count_ = 0;
				batch_size_ = batch_size;
			}
			~ParamBatchWrapper() {
				delete[] params_;
				params_ = NULL;
			}

			void push_back(TxnParam *tuple, const size_t &part_id) {
				assert(param_count_ < batch_size_);
				params_[param_count_].param_ = tuple;
				params_[param_count_].part_id_ = part_id;
				++param_count_;
			}

			size_t size() const {
				return param_count_;
			}

			ParamPtrWrapper* get(const size_t idx) const {
				return &(params_[idx]);
			}

		private:
			ParamPtrWrapper *params_;
			size_t param_count_;
			size_t batch_size_;
		};
	}
}

#endif
