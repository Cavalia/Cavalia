#pragma once
#ifndef __CAVALIA_DATABASE_TXN_PARAM_H__
#define __CAVALIA_DATABASE_TXN_PARAM_H__

#include <cassert>
#include <cstdint>
#include <vector>
#include <CharArray.h>

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
	};
}

#endif