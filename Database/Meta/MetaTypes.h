#pragma once
#ifndef __CAVALIA_DATABASE_META_TYPES_H__
#define __CAVALIA_DATABASE_META_TYPES_H__

#include <cstring>
#include <cstdint>

namespace Cavalia{
	namespace Database{

		extern size_t gParamBatchSize;
		extern size_t gAdhocRatio;

		const size_t kEventsNum = 2;
		const size_t kMaxProcedureNum = 10;
		const size_t kMaxThreadNum = 48;
		const size_t kMaxAccessNum = 256;
		const size_t kBatchTsNum = 16;
		const size_t kLogBufferSize = 8192000;
		const size_t kTxnBufferSize = 1024000;

		enum LockType : size_t{ NO_LOCK, READ_LOCK, WRITE_LOCK, CERTIFY_LOCK };
		enum AccessType : size_t { READ_ONLY, READ_WRITE, INSERT_ONLY, DELETE_ONLY };

		typedef uint32_t HashcodeType;

	}
}

#endif
