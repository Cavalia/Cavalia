#pragma once
#ifndef __CAVALIA_DATABASE_COLUMN_INFO_H__
#define __CAVALIA_DATABASE_COLUMN_INFO_H__

#include <string>
#include <cstdint>
#include <cassert>

namespace Cavalia{
	namespace Database{

		enum ValueType : size_t{ INT, INT8, INT16, INT32, INT64, DOUBLE, FLOAT, VARCHAR };

		const size_t kIntSize = sizeof(int);
		const size_t kInt8Size = sizeof(int8_t);
		const size_t kInt16Size = sizeof(int16_t);
		const size_t kInt32Size = sizeof(int32_t);
		const size_t kInt64Size = sizeof(int64_t);
		const size_t kFloatSize = sizeof(float);
		const size_t kDoubleSize = sizeof(double);

		struct ColumnInfo{
			ColumnInfo(const std::string &column_name, const ValueType &column_type) : column_name_(column_name), column_type_(column_type), column_offset_(0){
				assert(column_type != ValueType::VARCHAR);
				switch (column_type){
				case INT:
					column_size_ = kIntSize;
					break;
				case INT8:
					column_size_ = kInt8Size;
					break;
				case INT16:
					column_size_ = kInt16Size;
					break;
				case INT32:
					column_size_ = kInt32Size;
					break;
				case INT64:
					column_size_ = kInt64Size;
					break;
				case DOUBLE:
					column_size_ = kDoubleSize;
					break;
				case FLOAT:
					column_size_ = kFloatSize;
					break;
				default:
					break;
				}
			}

			ColumnInfo(const std::string &column_name, const ValueType &column_type, const size_t &column_size) : column_name_(column_name), column_type_(column_type), column_size_(column_size), column_offset_(0){}

			const std::string column_name_;
			const ValueType column_type_;
			size_t column_size_;
			size_t column_offset_;
		};
	}
}

#endif
