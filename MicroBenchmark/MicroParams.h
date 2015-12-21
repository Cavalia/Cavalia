#pragma once
#ifndef __CAVALIA_MICRO_BENCHMARK_MICRO_PARAMS_H__
#define __CAVALIA_MICRO_BENCHMARK_MICRO_PARAMS_H__

#include <Transaction/TxnParam.h>
#include "MicroRecords.h"
#include "MicroMeta.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Micro{
			using namespace Cavalia::Database;
			class AmalgamateParam : public TxnParam{
			public:
				AmalgamateParam(){
					type_ = AMALGAMATE;
				}
				virtual ~AmalgamateParam(){}

				virtual uint64_t GetHashCode() const{
					return -1;
				}
				virtual void Serialize(CharArray& serial_str) const {
					serial_str.Allocate(sizeof(int64_t) * 2);
					serial_str.Memcpy(0, reinterpret_cast<const char*>(&custid_0_), sizeof(int64_t));
					serial_str.Memcpy(sizeof(int64_t), reinterpret_cast<const char*>(&custid_1_), sizeof(int64_t));
				}
				virtual void Serialize(char *buffer, size_t &buffer_size) const {
					buffer_size = sizeof(int64_t) * 2;
					memcpy(buffer, reinterpret_cast<const char*>(&custid_0_), sizeof(int64_t));
					memcpy(buffer + sizeof(int64_t), reinterpret_cast<const char*>(&custid_1_), sizeof(int64_t));
				}
				virtual void Deserialize(const CharArray& serial_str) {
					memcpy(reinterpret_cast<char*>(&custid_0_), serial_str.char_ptr_, sizeof(int64_t));
					memcpy(reinterpret_cast<char*>(&custid_1_), serial_str.char_ptr_ + sizeof(int64_t), sizeof(int64_t));
				}
			public:
				int64_t custid_0_;
				int64_t custid_1_;
			};
		}
	}
}

#endif