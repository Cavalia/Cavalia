#pragma once
#ifndef __CAVALIA_STORAGE_ENGINE_SCHEMA_EXTRACTOR_H__
#define __CAVALIA_STORAGE_ENGINE_SCHEMA_EXTRACTOR_H__

#include <fstream>
#include "SchemaSerializer.pb.h"
#include "RecordSchema.h"

namespace Cavalia{
	namespace StorageEngine{
		class SchemaExtractor{
		public:
			RecordSchema* ExtractSchema(const std::string &filename){
				std::ifstream infile(filename);
				std::string str((std::istreambuf_iterator<char>(infile)),
					std::istreambuf_iterator<char>());
				infile.close();
				RecordSchema *schema = new RecordSchema();
				serializer_.ParseFromString(str);
				std::vector<ColumnInfo*> columns;
				for (int i = 0; i < serializer_.schemas_size(); ++i){
					const SchemaSerializer::Schema &tmp = serializer_.schemas(i);
					std::string field_name = tmp.field_name();
					if (tmp.field_type() == SchemaSerializer::Schema::INT){
						columns.push_back(new ColumnInfo(field_name, ValueType::INT));
					}
					else if (tmp.field_type() == SchemaSerializer::Schema::INT8){
						columns.push_back(new ColumnInfo(field_name, ValueType::INT8));
					}
					else if (tmp.field_type() == SchemaSerializer::Schema::INT16){
						columns.push_back(new ColumnInfo(field_name, ValueType::INT16));
					}
					else if (tmp.field_type() == SchemaSerializer::Schema::INT32){
						columns.push_back(new ColumnInfo(field_name, ValueType::INT32));
					}
					else if (tmp.field_type() == SchemaSerializer::Schema::INT64){
						columns.push_back(new ColumnInfo(field_name, ValueType::INT64));
					}
					else if (tmp.field_type() == SchemaSerializer::Schema::DOUBLE){
						columns.push_back(new ColumnInfo(field_name, ValueType::DOUBLE));
					}
					else if (tmp.field_type() == SchemaSerializer::Schema::FLOAT){
						columns.push_back(new ColumnInfo(field_name, ValueType::FLOAT));
					}
					else if (tmp.field_type() == SchemaSerializer::Schema::VARCHAR){
						columns.push_back(new ColumnInfo(field_name, ValueType::VARCHAR, tmp.field_size()));
					}
				}
				schema->InsertColumns(columns);
				if (serializer_.has_primary_key()){
					int size = serializer_.primary_key().fields_size();
					size_t *column_ids = new size_t[size];
					for (int i = 0; i < size; ++i){
						column_ids[i] = serializer_.primary_key().fields(i);
					}
					schema->SetPrimaryColumns(column_ids, size);
					delete[] column_ids;
					column_ids = NULL;
				}
				for (int i = 0; i < serializer_.secondary_keys_size(); ++i){
					const SchemaSerializer::Key &tmp = serializer_.secondary_keys(i);
					int size = tmp.fields_size();
					size_t *column_ids = new size_t[size];
					for (int j = 0; j < size; ++j){
						column_ids[j] = tmp.fields(j);
					}
					schema->AddSecondaryColumns(column_ids, size);;
					delete[] column_ids;
					column_ids = NULL;
				}
				return schema;
			}

		private:
			SchemaSerializer serializer_;
		};
	}
}

#endif