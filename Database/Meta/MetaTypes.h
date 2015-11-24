#pragma once
#ifndef __CAVALIA_DATABASE_META_TYPES_H__
#define __CAVALIA_DATABASE_META_TYPES_H__

#include <cstring>
#include <string>
#include <cstdint>
#include <cassert>
#include <AllocatorHelper.h>
#include <EventTuple.h>

#if defined(MUTE)
#define MEASURE_BEGIN() ;

#define MEASURE_END(TASK) ;

#define MEASURE_END_LAYER(TASK, numLayer) ;

#define MEASURE_END_PARTITION(TASK, numPartition) ;

#define MEASURE_END_LAYER_PARTITION(TASK, numLayer, numPartition) ;
#else
#define MEASURE_BEGIN()\
	TimeMeasurer timer; \
	timer.StartTimer();

#define MEASURE_END(TASK)\
	timer.EndTimer(); \
	std::cout << #TASK" consumed " << timer.GetElapsedMilliSeconds() << " ms." << std::endl;

#define MEASURE_END_LAYER(TASK, numLayer)\
	timer.EndTimer(); \
	std::cout << #TASK" <layer " << numLayer << "> consumed " << timer.GetElapsedMilliSeconds() << " ms." << std::endl;

#define MEASURE_END_PARTITION(TASK, numPartition)\
	timer.EndTimer(); \
	std::cout << #TASK" <partition " << numPartition << "> consumed " << timer.GetElapsedMilliSeconds() << " ms." << std::endl;

#define MEASURE_END_LAYER_PARTITION(TASK, numLayer, numPartition)\
	timer.EndTimer(); \
	std::cout << #TASK" <layer " << numLayer << ", partition " << numPartition << "> consumed " << timer.GetElapsedMilliSeconds() << " ms." << std::endl;
#endif



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
		
		enum ValueType : size_t{ INT, INT8, INT16, INT32, INT64, DOUBLE, FLOAT, VARCHAR };
		enum IndexType : size_t{ HASHMAP, BTREE };
		enum LockType : size_t{ NO_LOCK, READ_LOCK, WRITE_LOCK, CERTIFY_LOCK };
		enum AccessType : size_t { READ_ONLY, READ_WRITE, DELETE_ONLY };
		enum SourceType : size_t { RANDOM_SOURCE, PARTITION_SOURCE };

		const size_t kIntSize = sizeof(int);
		const size_t kInt8Size = sizeof(int8_t);
		const size_t kInt16Size = sizeof(int16_t);
		const size_t kInt32Size = sizeof(int32_t);
		const size_t kInt64Size = sizeof(int64_t);
		const size_t kFloatSize = sizeof(float);
		const size_t kDoubleSize = sizeof(double);

		const uint8_t kInsert = 0;
		const uint8_t kUpdate = 1;
		const uint8_t kDelete = 2;

		class TupleBatch {
		public:
			TupleBatch() {
				tuples_ = new EventTuple*[gTupleBatchSize];
				tuple_count_ = 0;
				batch_size_ = gTupleBatchSize;
			}
			TupleBatch(const size_t &batch_size) {
				tuples_ = new EventTuple*[batch_size];
				tuple_count_ = 0;
				batch_size_ = batch_size;
			}
			~TupleBatch() {
				delete[] tuples_;
				tuples_ = NULL;
			}

			void push_back(EventTuple *tuple) {
				assert(tuple_count_ < batch_size_);
				tuples_[tuple_count_] = tuple;
				++tuple_count_;
			}

			size_t size() const {
				return tuple_count_;
			}

			EventTuple* get(const size_t idx) const {
				return tuples_[idx];
			}

		private:
			EventTuple **tuples_;
			size_t tuple_count_;
			size_t batch_size_;
		};

		struct TuplePtrWrapper{
			size_t part_id_;
			EventTuple *tuple_;
		};

		class TupleBatchWrapper {
		public:
			TupleBatchWrapper() {
				tuples_ = new TuplePtrWrapper[gTupleBatchSize];
				tuple_count_ = 0;
				batch_size_ = gTupleBatchSize;
			}
			TupleBatchWrapper(const size_t &batch_size) {
				tuples_ = new TuplePtrWrapper[batch_size];
				tuple_count_ = 0;
				batch_size_ = batch_size;
			}
			~TupleBatchWrapper() {
				delete[] tuples_;
				tuples_ = NULL;
			}

			void push_back(EventTuple *tuple, const size_t &part_id) {
				assert(tuple_count_ < batch_size_);
				tuples_[tuple_count_].tuple_ = tuple;
				tuples_[tuple_count_].part_id_ = part_id;
				++tuple_count_;
			}

			size_t size() const {
				return tuple_count_;
			}

			TuplePtrWrapper* get(const size_t idx) const {
				return &(tuples_[idx]);
			}

		private:
			TuplePtrWrapper *tuples_;
			size_t tuple_count_;
			size_t batch_size_;
		};


		typedef uint32_t HashcodeType;

		struct ColumnInfo{
			ColumnInfo(const std::string &column_name, const ValueType &column_type) : column_name_(column_name), column_type_(column_type), column_offset_(0){
				assert(column_type != ValueType::VARCHAR);
				switch (column_type){
				case INT:
					column_size_ = kIntSize;
					break;
				case INT8:
					column_size_ = kInt8Size;
					break;
				case INT16:
					column_size_ = kInt16Size;
					break;
				case INT32:
					column_size_ = kInt32Size;
					break;
				case INT64:
					column_size_ = kInt64Size;
					break;
				case DOUBLE:
					column_size_ = kDoubleSize;
					break;
				case FLOAT:
					column_size_ = kFloatSize;
					break;
				default:
					break;
				}
			}

			ColumnInfo(const std::string &column_name, const ValueType &column_type, const size_t &column_size) : column_name_(column_name), column_type_(column_type), column_size_(column_size), column_offset_(0){}

			const std::string column_name_;
			const ValueType column_type_;
			size_t column_size_;
			size_t column_offset_;
		};

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

		struct SliceLocation{
			size_t GetPartitionCount() const {
				if (core_ids_.size() != 0){
					return core_ids_.size();
				}
				else{
					return node_ids_.size();
				}
			}
			std::vector<size_t> core_ids_;
			std::vector<size_t> node_ids_;
		};

		struct PoolLocation{
			size_t GetCoreCount() const {
				return core_ids_.size();
			}
			std::vector<size_t> core_ids_;
		};

		extern MemAllocator *allocator_;
		
	}
}

#endif
