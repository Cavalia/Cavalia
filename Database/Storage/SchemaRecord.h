#pragma once
#ifndef __CAVALIA_DATABASE_SCHEMA_RECORD_H__
#define __CAVALIA_DATABASE_SCHEMA_RECORD_H__

#include <CharArray.h>
#include "RecordSchema.h"

namespace Cavalia{
	namespace Database{
		class SchemaRecord{
		public:
			SchemaRecord() : schema_ptr_(NULL), is_visible_(true), data_ptr_(NULL) {}
			SchemaRecord(const RecordSchema * const schema_ptr, char *data_ptr) : schema_ptr_(schema_ptr), is_visible_(true), data_ptr_(data_ptr) {}
			~SchemaRecord(){}

			size_t GetTableId() const{
				return schema_ptr_->GetTableId();
			}

			void CopyTo(const SchemaRecord *dst_record){
				memcpy(dst_record->data_ptr_, data_ptr_, schema_ptr_->GetSchemaSize());
			}

			void CopyFrom(const SchemaRecord *src_record){
				memcpy(data_ptr_, src_record->data_ptr_, schema_ptr_->GetSchemaSize());
			}

			void SwapData(SchemaRecord *src_record){
				char *tmp_ptr = data_ptr_;
				data_ptr_ = src_record->data_ptr_;
				src_record->data_ptr_ = tmp_ptr;
			}

			// set column. type can be any type.
			void SetColumn(const size_t &column_id, const char *data){
				memcpy(data_ptr_ + schema_ptr_->GetColumnOffset(column_id), data, schema_ptr_->GetColumnSize(column_id));
			}

			// rename.
			void UpdateColumn(const size_t &column_id, const char*data){
				memcpy(data_ptr_ + schema_ptr_->GetColumnOffset(column_id), data, schema_ptr_->GetColumnSize(column_id));
			}

			// set column. type must be varchar.
			void SetColumn(const size_t &column_id, const CharArray &data){
				assert(schema_ptr_->GetColumnType(column_id) == ValueType::VARCHAR && schema_ptr_->GetColumnSize(column_id) >= data.size_);
				memcpy(data_ptr_ + schema_ptr_->GetColumnOffset(column_id), data.char_ptr_, data.size_);
			}

			// set column. type must be varchar.
			void SetColumn(const size_t &column_id, const char *data_str, const size_t &data_size){
				assert(schema_ptr_->GetColumnType(column_id) == ValueType::VARCHAR && schema_ptr_->GetColumnSize(column_id) >= data_size);
				memcpy(data_ptr_ + schema_ptr_->GetColumnOffset(column_id), data_str, data_size);
			}

			// set column. type must be varchar.
			void SetColumn(const size_t &column_id, const std::string &data){
				assert(schema_ptr_->GetColumnType(column_id) == ValueType::VARCHAR && schema_ptr_->GetColumnSize(column_id) >= data.size());
				memcpy(data_ptr_ + schema_ptr_->GetColumnOffset(column_id), data.c_str(), data.size());
			}

			// reference
			char* GetColumn(const size_t &column_id) const {
				return data_ptr_ + schema_ptr_->GetColumnOffset(column_id);
			}

			// copy data, memory allocated outside
			char* GetColumn(const size_t &column_id, char *data) const {
				assert(schema_ptr_->GetColumnType(column_id) != ValueType::VARCHAR);
				memcpy(data, data_ptr_ + schema_ptr_->GetColumnOffset(column_id), schema_ptr_->GetColumnSize(column_id));
				return data;
			}

			// copy data, memory allocated inside
			CharArray& GetColumn(const size_t &column_id, CharArray &data) const {
				assert(schema_ptr_->GetColumnType(column_id) == ValueType::VARCHAR);
				data.size_ = schema_ptr_->GetColumnSize(column_id);
				data.char_ptr_ = new char[data.size_];
				memcpy(data.char_ptr_, data_ptr_ + schema_ptr_->GetColumnOffset(column_id), data.size_);
				return data;
			}

			// copy data, memory allocated inside
			void GetColumn(const size_t &column_id, std::string &data) const {
				assert(schema_ptr_->GetColumnType(column_id) == ValueType::VARCHAR);
				data.assign(data_ptr_ + schema_ptr_->GetColumnOffset(column_id), 0, schema_ptr_->GetColumnSize(column_id));
			}

			const size_t& GetColumnSize(const size_t &column_id) const {
				return schema_ptr_->GetColumnSize(column_id);
			}

			const size_t& GetRecordSize() const {
				return schema_ptr_->GetSchemaSize();
			}

			const size_t& GetColumnCount() const {
				return schema_ptr_->GetColumnCount();
			}

			std::string GetPrimaryKey() const {
				size_t curr_offset = 0;
				size_t key_length = schema_ptr_->GetPrimaryKeyLength();
				if (key_length == 0){
					return std::string(data_ptr_, schema_ptr_->GetSchemaSize());
				}
				char *key_str = new char[key_length];
				for (size_t i = 0; i < schema_ptr_->GetPrimaryColumnCount(); ++i){
					memcpy(key_str + curr_offset, GetColumn(schema_ptr_->GetPrimaryColumnId(i)), schema_ptr_->GetPrimaryColumnSize(i));
					curr_offset += schema_ptr_->GetPrimaryColumnSize(i);
				}
				std::string ret_key(key_str, key_length);
				delete[] key_str;
				key_str = NULL;
				return ret_key;
			}

			std::string GetSecondaryKey(const size_t &index) const {
				size_t curr_offset = 0;
				size_t key_length = schema_ptr_->GetSecondaryKeyLength(index);
				if (key_length == 0){
					return std::string(data_ptr_, schema_ptr_->GetSchemaSize());
				}
				char *key_str = new char[key_length];
				for (size_t i = 0; i < schema_ptr_->GetSecondaryColumnCount(index); ++i){
					memcpy(key_str + curr_offset, GetColumn(schema_ptr_->GetSecondaryColumnId(index, i)), schema_ptr_->GetSecondaryColumnSize(index, i));
					curr_offset += schema_ptr_->GetSecondaryColumnSize(index, i);
				}
				std::string ret_key(key_str, key_length);
				delete[] key_str;
				key_str = NULL;
				return ret_key;
			}

			HashcodeType GetPartitionHashcode() const {
				HashcodeType hashcode = 0;
				for (size_t i = 0; i < schema_ptr_->GetPartitionColumnCount(); ++i){
					hashcode += *(HashcodeType*)GetColumn(schema_ptr_->GetPartitionColumnId(i));
				}
				return hashcode;
			}

		private:
			SchemaRecord(const SchemaRecord&);
			SchemaRecord& operator=(const SchemaRecord&);

		public:
			char *data_ptr_;
			const RecordSchema *schema_ptr_;
			bool is_visible_;
		};
	}
}

#endif
