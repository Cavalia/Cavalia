#include "MetaTypes.h"


namespace Cavalia{
	namespace Database{
		MemAllocator *allocator_ = new MemAllocator();
		size_t gTupleBatchSize = 1000;
		size_t gAdhocRatio = 0;
	}
}