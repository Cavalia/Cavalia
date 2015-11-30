#pragma once
#ifndef __CAVALIA_DATABASE_ACCESS_H__
#define __CAVALIA_DATABASE_ACCESS_H__

#include <string>
#include <algorithm>
#include "../Meta/MetaTypes.h"
#include "../Storage/SchemaRecord.h"
#include "../Storage/TableRecord.h"

namespace Cavalia{
	namespace Database{
		struct Access{
			Access() : access_record_(NULL), local_record_(NULL){}
			AccessType access_type_;
			TableRecord *access_record_;
			SchemaRecord *local_record_;
			size_t table_id_;
#if defined(LOCK) || defined(OCC) || defined(SILO) || defined(MVOCC) || defined(HYBRID) || defined(DBX)
			uint64_t timestamp_;
#endif
		};


		template<int N>
		struct AccessList{
			AccessList() : access_count_(0){}

			Access *NewAccess(){
				assert(access_count_ < N);
				Access *ret = &(accesses_[access_count_]);
				++access_count_;
				return ret;
			}

			Access *GetAccess(const size_t &index){
				return &(accesses_[index]);
			}

			void Clear(){
				access_count_ = 0;
			}

			void Sort(){
				std::sort(accesses_, accesses_ + access_count_, CompFunction);
			}

			Access accesses_[N];
			size_t access_count_;

		private:
			static bool CompFunction(Access lhs, Access rhs){
				return (uint64_t)(lhs.access_record_) < (uint64_t)(rhs.access_record_);
			}
		};

		template<int N>
		struct WritePtrList{
			WritePtrList() : access_count_(0) {}

			void Add(Access *access) {
				assert(access_count_ < N);
				accesses_[access_count_] = access;
				++access_count_;
			}

			void Clear() {
				access_count_ = 0;
			}

			void Sort(){
				std::sort(accesses_, accesses_ + access_count_, CompFunction);
			}

			Access *accesses_[N];
			size_t access_count_;

		private:
			static bool CompFunction(Access *lhs, Access *rhs){
				return (uint64_t)(lhs->access_record_) < (uint64_t)(rhs->access_record_);
			}
		};

		template<int N>
		struct AccessPtrList{
			AccessPtrList() {}

			void Add(const size_t &access_id, Access *access) {
				assert(access_id < N);
				accesses_[access_id] = access;
			}

			void Clear(){
				memset(accesses_, 0, sizeof(Access*)*N);
			}

			Access *accesses_[N];
		};

		template<int N, int M>
		struct AccessPtrsList{
			AccessPtrsList(){
				memset(access_counts_, 0, sizeof(size_t)*N);
			}

			void Add(const size_t &access_id, Access *access) {
				assert(access_id < N);
				assert(access_counts_[access_id] < M);
				accesses_[access_id][access_counts_[access_id]] = access;
				++access_counts_[access_id];
			}

			void Clear() {
				memset(accesses_, 0, sizeof(Access*)*(N*M));
				memset(access_counts_, 0, sizeof(size_t)*N);
			}

			Access* accesses_[N][M];
			size_t access_counts_[N]; // how many items touched by each access.
		};
	}
}

#endif
