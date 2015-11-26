#pragma once
#ifndef __CAVALIA_DATABASE_RECORD_SCHEMA_H__
#define __CAVALIA_DATABASE_RECORD_SCHEMA_H__

#include <vector>
#include <cassert>
#include <cstring>
#include <unordered_map>
#include <Toolkits.h>
#include "ColumnInfo.h"
#include "../Meta/MetaTypes.h"

namespace Cavalia{
	namespace Database{
		class RecordSchema{
		public:
			RecordSchema(const size_t &table_id) : table_id_(table_id), column_count_(0), column_offset_(0){
				primary_key_length_ = 0;
				primary_col_num_ = 0;
				primary_col_length_ = 0;
				memset(primary_col_ids_, 0, sizeof(size_t)* 5);
				memset(primary_col_sizes_, 0, sizeof(size_t)* 5);

				partition_key_length_ = 0;
				partition_col_num_ = 0;
				partition_col_length_ = 0;
				memset(partition_col_ids_, 0, sizeof(size_t)* 5);
				memset(partition_col_sizes_, 0, sizeof(size_t)* 5);

				memset(secondary_key_length_, 0, sizeof(size_t)* 5);
				memset(secondary_col_num_, 0, sizeof(size_t)* 5);
				memset(secondary_col_length_, 0, sizeof(size_t)* 5);
				memset(secondary_col_ids_, 0, sizeof(size_t)* 25);
				memset(secondary_col_sizes_, 0, sizeof(size_t)* 25);

				secondary_num_ = 0;
			}
			~RecordSchema(){
				for (size_t i = 0; i < column_count_; ++i){
					delete columns_[i];
					columns_[i] = NULL;
				}
				delete[] columns_;
				columns_ = NULL;
			}

			size_t GetTableId() const {
				return table_id_;
			}

			void InsertColumns(const std::vector<ColumnInfo*> &columns){
				column_count_ = columns.size();
				columns_ = new ColumnInfo*[column_count_];
				for (size_t i = 0; i < column_count_; ++i){
					columns_[i] = columns[i];
					columns_[i]->column_offset_ = column_offset_;
					column_offset_ += columns_[i]->column_size_;
				}
			}

			void SetPrimaryColumns(const size_t *column_ids, const size_t &column_num){
				assert(column_num < 5);
				for (size_t i = 0; i < column_num; ++i){
					primary_key_length_ += columns_[column_ids[i]]->column_size_;
					primary_col_ids_[i] = column_ids[i];
					primary_col_sizes_[i] = columns_[column_ids[i]]->column_size_;
				}
				primary_col_num_ = column_num;
				primary_col_length_ = column_num * sizeof(size_t);
				primary_symbol_ = std::string((char*)(primary_col_ids_), sizeof(size_t)*primary_col_num_);
			}

			void SetPartitionColumns(const size_t *column_ids, const size_t &column_num){
				assert(column_num < 5);
				for (size_t i = 0; i < column_num; ++i){
					partition_key_length_ += columns_[column_ids[i]]->column_size_;
					partition_col_ids_[i] = column_ids[i];
					partition_col_sizes_[i] = columns_[column_ids[i]]->column_size_;
				}
				partition_col_num_ = column_num;
				partition_col_length_ = column_num * sizeof(size_t);
				partition_symbol_ = std::string((char*)(partition_col_ids_), sizeof(size_t)*partition_col_num_);
			}

			void AddSecondaryColumns(const size_t *column_ids, const size_t &column_num){
				assert(column_num < 5);
				for (size_t i = 0; i < column_num; ++i){
					secondary_key_length_[secondary_num_] += columns_[column_ids[i]]->column_size_;
					secondary_col_ids_[secondary_num_][i] = column_ids[i];
					secondary_col_sizes_[secondary_num_][i] = columns_[column_ids[i]]->column_size_;
				}
				secondary_col_num_[secondary_num_] = column_num;
				secondary_col_length_[secondary_num_] = column_num * sizeof(size_t);
				secondary_symbols_[std::string((char*)(column_ids), sizeof(size_t)*column_num)] = secondary_num_;
				secondary_num_ += 1;
			}

			const ColumnInfo& GetColumn(const size_t &column_id) const{
				assert(column_id < column_count_);
				return *columns_[column_id];
			}

			const size_t& GetSchemaSize()const{ return column_offset_; }
			const size_t& GetColumnCount()const{ return column_count_; }

			//const std::string& GetColumnName(const size_t &index)const{
			//	assert(index < column_count_);
			//	return columns_[index]->column_name_;
			//}

			const ValueType& GetColumnType(const size_t &index)const{
				assert(index < column_count_);
				return columns_[index]->column_type_;
			}

			const size_t& GetColumnOffset(const size_t &index)const{
				assert(index < column_count_);
				return columns_[index]->column_offset_;
			}

			const size_t& GetColumnSize(const size_t &index)const{
				assert(index < column_count_);
				return columns_[index]->column_size_;
			}

			const size_t& GetPrimaryColumnCount()const{ return primary_col_num_; }

			const size_t& GetPartitionColumnCount()const{ return partition_col_num_; }

			const size_t& GetSecondaryColumnCount(const size_t &index)const{ return secondary_col_num_[index]; }

			const size_t& GetSecondaryCount()const{ return secondary_num_; }

			const size_t& GetPrimaryColumnId(const size_t &col_index)const{
				assert(col_index < primary_col_num_);
				return primary_col_ids_[col_index];
			}

			const size_t& GetPartitionColumnId(const size_t &col_index)const{
				assert(col_index < partition_col_num_);
				return partition_col_ids_[col_index];
			}

			const size_t& GetSecondaryColumnId(const size_t &index, const size_t &col_index)const{
				return secondary_col_ids_[index][col_index];
			}

			const size_t& GetPrimaryColumnSize(const size_t &col_index)const{
				assert(col_index < primary_col_num_);
				return primary_col_sizes_[col_index];
			}

			const size_t& GetPartitionColumnSize(const size_t &col_index)const{
				assert(col_index < partition_col_num_);
				return partition_col_sizes_[col_index];
			}

			const size_t& GetSecondaryColumnSize(const size_t &index, const size_t &col_index)const{
				return secondary_col_sizes_[index][col_index];
			}

			const size_t& GetPrimaryKeyLength()const{ return primary_key_length_; }

			const size_t& GetPartitionKeyLength()const{ return partition_key_length_; }

			const size_t& GetSecondaryKeyLength(const size_t &index)const{ return secondary_key_length_[index]; }

			size_t GetKeyLength(const size_t *index_cols, const size_t &index_col_num)const{
				size_t sum = 0;
				for (size_t i = 0; i < index_col_num; ++i){
					sum += columns_[index_cols[i]]->column_size_;
				}
				return sum;
			}

			bool IsPrimaryKey(std::string &symbol)const{
				return symbol == primary_symbol_;
			}

			int IsSecondaryKey(std::string &symbol)const{
				if (secondary_symbols_.find(symbol) != secondary_symbols_.end()){
					return secondary_symbols_.at(symbol);
				}
				else{
					return -1;
				}
			}

			HashcodeType GetPartitionHashcode(const std::string &primary_key) const {
				HashcodeType hashcode = 0;
				size_t col_count = 0;
				size_t primary_offset = 0;
				for (size_t i = 0; i < primary_col_num_; ++i){
					if (primary_col_ids_[i] == partition_col_ids_[col_count]){
						hashcode += *(HashcodeType*)(primary_key.c_str() + primary_offset);
						++col_count;
					}
					primary_offset += primary_col_sizes_[i];
				}
				return hashcode;
			}

			HashcodeType GetPartitionHashcode(const size_t &idx_id, const std::string &secondary_key) const {
				HashcodeType hashcode = 0;
				size_t col_count = 0;
				size_t offset = 0;
				for (size_t i = 0; i < secondary_col_num_[idx_id]; ++i){
					size_t col_id = secondary_col_ids_[idx_id][i];
					size_t col_size = columns_[col_id]->column_size_;
					if (col_id == partition_col_ids_[col_count]){
						hashcode += *(HashcodeType*)(secondary_key.substr(offset, col_size).c_str());
						++col_count;
						if (col_count == partition_col_num_){
							return hashcode;
						}
					}
					offset += col_size;
				}
				assert(false);
				return hashcode;
			}

		private:
			RecordSchema(const RecordSchema &);
			RecordSchema& operator=(const RecordSchema &);

		private:
			size_t table_id_;
			ColumnInfo **columns_;
			size_t column_count_;
			size_t column_offset_;

			size_t primary_key_length_;
			size_t primary_col_num_;
			size_t primary_col_length_; // col_length = col_num * sizeof(size_t)
			size_t primary_col_ids_[5];
			size_t primary_col_sizes_[5];
			std::string primary_symbol_;

			size_t partition_key_length_;
			size_t partition_col_num_;
			size_t partition_col_length_; // col_length = col_num * sizeof(size_t)
			size_t partition_col_ids_[5];
			size_t partition_col_sizes_[5];
			std::string partition_symbol_;

			size_t secondary_key_length_[5];
			size_t secondary_col_num_[5];
			size_t secondary_col_length_[5]; // col_length = col_num * sizeof(size_t)
			size_t secondary_col_ids_[5][5];
			size_t secondary_col_sizes_[5][5];
			std::unordered_map<std::string, size_t> secondary_symbols_;
			size_t secondary_num_;
		};
	}
}

#endif
