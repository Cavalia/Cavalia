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
				static RecordSchema* GenerateMicroSchema(){
					if (micro_schema_ == NULL) {
						micro_schema_ = new RecordSchema(MICRO_TABLE_ID);
						std::vector<ColumnInfo*> columns;
						columns.push_back(new ColumnInfo("key", ValueType::INT64));
						columns.push_back(new ColumnInfo("value", ValueType::VARCHAR, static_cast<size_t>(64)));
						micro_schema_->InsertColumns(columns);
						size_t column_ids[] = { 0 };
						micro_schema_->SetPrimaryColumns(column_ids, 1);
					}
					return micro_schema_;
				}

			private:
				MicroSchema();
				MicroSchema(const MicroSchema&);
				MicroSchema& operator=(const MicroSchema&);

			private:
				static RecordSchema *micro_schema_;
			};
		}
	}
}

#endif
