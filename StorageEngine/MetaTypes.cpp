#include "MetaTypes.h"


namespace Cavalia{
	namespace StorageEngine{
		MemAllocator *allocator_ = new MemAllocator();
		size_t gTupleBatchSize = 1000;
		size_t gAdhocRatio = 0;
	}
}