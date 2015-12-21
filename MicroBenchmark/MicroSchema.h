#pragma once
#ifndef __CAVALIA_MICRO_BENCHMARK_MICRO_SCHEMA_H__
#define __CAVALIA_MICRO_BENCHMARK_MICRO_SCHEMA_H__

#include <Storage/RecordSchema.h>
#include "MicroMeta.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Micro{
			using namespace Cavalia::Database;
			class MicroSchema{
			public:
				static RecordSchema* GenerateAccountsSchema(){
					if (accounts_schema_ == NULL) {
						accounts_schema_ = new RecordSchema(ACCOUNTS_TABLE_ID);
						std::vector<ColumnInfo*> columns;
						columns.push_back(new ColumnInfo("custid", ValueType::INT64));
						columns.push_back(new ColumnInfo("name", ValueType::VARCHAR, static_cast<size_t>(64)));
						accounts_schema_->InsertColumns(columns);
						size_t column_ids[] = { 0 };
						accounts_schema_->SetPrimaryColumns(column_ids, 1);
						//size_t name_column_ids[] = { 1 };
						//accounts_schema_->AddSecondaryColumns(name_column_ids, 1);
					}
					return accounts_schema_;
				}

				static RecordSchema* GenerateSavingsSchema(){
					if (savings_schema_ == NULL) {
						savings_schema_ = new RecordSchema(SAVINGS_TABLE_ID);
						std::vector<ColumnInfo*> columns;
						columns.push_back(new ColumnInfo("custid", ValueType::INT64));
						columns.push_back(new ColumnInfo("bal", ValueType::FLOAT));
						savings_schema_->InsertColumns(columns);
						size_t column_ids[] = { 0 };
						savings_schema_->SetPrimaryColumns(column_ids, 1);
					}
					return savings_schema_;
				}

				static RecordSchema* GenerateCheckingSchema(){
					if (checking_schema_ == NULL) {
						checking_schema_ = new RecordSchema(CHECKING_TABLE_ID);
						std::vector<ColumnInfo*> columns;
						columns.push_back(new ColumnInfo("custid", ValueType::INT64));
						columns.push_back(new ColumnInfo("bal", ValueType::FLOAT));
						checking_schema_->InsertColumns(columns);
						size_t column_ids[] = { 0 };
						checking_schema_->SetPrimaryColumns(column_ids, 1);
					}
					return checking_schema_;
				}

			private:
				MicroSchema();
				MicroSchema(const MicroSchema&);
				MicroSchema& operator=(const MicroSchema&);

			private:
				static RecordSchema *accounts_schema_;
				static RecordSchema *savings_schema_;
				static RecordSchema *checking_schema_;
			};
		}
	}
}

#endif
