#pragma once
#ifndef __CAVALIA_SMALLBANK_BENCHMARK_SMALLBANK_SCHEMA_H__
#define __CAVALIA_SMALLBANK_BENCHMARK_SMALLBANK_SCHEMA_H__

#include <Storage/RecordSchema.h>
#include "SmallbankMeta.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Smallbank{
			using namespace Cavalia::Database;
			class SmallbankSchema{
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
				SmallbankSchema();
				SmallbankSchema(const SmallbankSchema&);
				SmallbankSchema& operator=(const SmallbankSchema&);

			private:
				static RecordSchema *accounts_schema_;
				static RecordSchema *savings_schema_;
				static RecordSchema *checking_schema_;
			};
		}
	}
}

#endif
