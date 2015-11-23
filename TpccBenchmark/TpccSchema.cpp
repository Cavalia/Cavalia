#include "TpccSchema.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Tpcc{
			RecordSchema* TpccSchema::item_schema_ = NULL;
			RecordSchema* TpccSchema::warehouse_schema_ = NULL;
			RecordSchema* TpccSchema::district_schema_ = NULL;
			RecordSchema* TpccSchema::customer_schema_ = NULL;
			RecordSchema* TpccSchema::order_schema_ = NULL;
			RecordSchema* TpccSchema::new_order_schema_ = NULL;
			RecordSchema* TpccSchema::order_line_schema_ = NULL;
			RecordSchema* TpccSchema::history_schema_ = NULL;
			RecordSchema* TpccSchema::stock_schema_ = NULL;
			RecordSchema* TpccSchema::district_new_order_schema_ = NULL;
		}
	}
}
