#pragma once
#ifndef __CAVALIA_BENCHMARK_COMMON_BENCHMARK_TUPLES_H__
#define __CAVALIA_BENCHMARK_COMMON_BENCHMARK_TUPLES_H__

#include "../Transaction/TxnParam.h"

namespace Cavalia{
	namespace Benchmark{

#define TUPLE_CLASS(TupleName) \
		class TupleName##Tuple : public TxnParam{ \
		public: \
		TupleName##Tuple() { \
		memset(reinterpret_cast<char*>(&data_), 0, sizeof(data_)); \
		} \
		virtual ~TupleName##Tuple(){} \
		virtual uint64_t GetHashCode() const { \
		return -1; \
		} \
		virtual void Serialize(CharArray& serial_str) const { \
			serial_str.Allocate(sizeof(TupleName##Record)); \
			serial_str.Memcpy(0, reinterpret_cast<const char*>(&data_), sizeof(TupleName##Record)); \
		} \
		virtual void Deserialize(const CharArray& serial_str) { \
			memcpy(&data_, serial_str.char_ptr_, serial_str.size_); \
		} \
		public: \
		TupleName##Record data_; \
		}
	}
}


#endif