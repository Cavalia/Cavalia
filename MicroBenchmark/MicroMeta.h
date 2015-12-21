#pragma once
#ifndef __CAVALIA_MICRO_BENCHMARK_MICRO_META_H__
#define __CAVALIA_MICRO_BENCHMARK_MICRO_META_H__

namespace Cavalia{
	namespace Benchmark{
		namespace Micro{
			enum TupleType : size_t { 
				AMALGAMATE, 
			};
			
			enum TableType : size_t { 
				MICRO_TABLE_ID, 
				kTableCount 
			};
		}
	}
}

#endif
