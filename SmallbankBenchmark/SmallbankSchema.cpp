#include "SmallbankSchema.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Smallbank{
			RecordSchema* SmallbankSchema::accounts_schema_ = NULL;
			RecordSchema* SmallbankSchema::savings_schema_ = NULL;
			RecordSchema* SmallbankSchema::checking_schema_ = NULL;
		}
	}
}
