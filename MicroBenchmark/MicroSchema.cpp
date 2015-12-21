#include "MicroSchema.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Micro{
			RecordSchema* MicroSchema::accounts_schema_ = NULL;
			RecordSchema* MicroSchema::savings_schema_ = NULL;
			RecordSchema* MicroSchema::checking_schema_ = NULL;
		}
	}
}
