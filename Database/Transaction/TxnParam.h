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
				tuples_ = new TxnParam*[gTupleBatchSize];
				tuple_count_ = 0;
				batch_size_ = gTupleBatchSize;
			}
			ParamBatch(const size_t &batch_size) {
				tuples_ = new TxnParam*[batch_size];
				tuple_count_ = 0;
				batch_size_ = batch_size;
			}
			~ParamBatch() {
				delete[] tuples_;
				tuples_ = NULL;
			}

			void push_back(TxnParam *tuple) {
				assert(tuple_count_ < batch_size_);
				tuples_[tuple_count_] = tuple;
				++tuple_count_;
			}

			size_t size() const {
				return tuple_count_;
			}

			TxnParam* get(const size_t idx) const {
				return tuples_[idx];
			}

		private:
			TxnParam **tuples_;
			size_t tuple_count_;
			size_t batch_size_;
		};

	}
}

#endif
