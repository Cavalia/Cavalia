#pragma once
#ifndef __CAVALIA_DATABASE_META_TYPES_H__
#define __CAVALIA_DATABASE_META_TYPES_H__

#include <vector>
#include <cstring>
#include <string>
#include <cstdint>
#include <cassert>
#include <AllocatorHelper.h>

namespace Cavalia{
	namespace Database{

		extern size_t gTupleBatchSize;
		extern size_t gAdhocRatio;

		const size_t kRetryTime = 3;
		const size_t kEventsNum = 2;
		const size_t kTablePartitionNum = 128;
		const size_t kMaxProcedureNum = 10;
		const size_t kMaxSliceNum = 21;
		const size_t kMaxThreadNum = 48;
		const size_t kRecvTimeout = 1000;
		const size_t kMaxAccessNum = 256;
		const size_t kMaxAccessPerTableNum = 256;
		const size_t kMaxOptPerTableNum = 16;
		const size_t kMaxAccessPerOptNum = 16;
		const size_t kBatchTsNum = 16;
		const size_t kStrBufferSize = 256;
		const size_t kValueLogBufferSize = 4096000;
		const size_t kCommandLogBufferSize = 4096;
		
		enum LockType : size_t{ NO_LOCK, READ_LOCK, WRITE_LOCK, CERTIFY_LOCK };
		enum AccessType : size_t { READ_ONLY, READ_WRITE, DELETE_ONLY };
		enum SourceType : size_t { RANDOM_SOURCE, PARTITION_SOURCE };


		typedef uint32_t HashcodeType;

		struct TableLocation{
			size_t GetPartitionCount() const {
				return node_ids_.size();
			}
			std::vector<size_t> node_ids_;
		};

		struct TxnLocation{
			size_t GetCoreCount() const {
				return core_ids_.size();
			}
			std::vector<size_t> core_ids_;
			size_t node_count_;
		};
	}
}

#endif
