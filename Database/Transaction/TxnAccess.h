#pragma once
#ifndef __CAVALIA_DATABASE_TXN_ACCESS_H__
#define __CAVALIA_DATABASE_TXN_ACCESS_H__

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
			uint64_t timestamp_;
		};

		template<int N>
		class AccessList{
		public:
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

		private:
			static bool CompFunction(Access lhs, Access rhs){
				return (uint64_t)(lhs.access_record_) < (uint64_t)(rhs.access_record_);
			}

		public:
			size_t access_count_;
		private:
			Access accesses_[N];
		};

		template<int N>
		class AccessPtrList{
		public:
			AccessPtrList() : access_count_(0) {}

			void Add(Access *access) {
				assert(access_count_ < N);
				accesses_[access_count_] = access;
				++access_count_;
			}

			Access *GetAccess(const size_t &index){
				return accesses_[index];
			}

			void Clear() {
				access_count_ = 0;
			}

			void Sort(){
				std::sort(accesses_, accesses_ + access_count_, CompFunction);
			}

		private:
			static bool CompFunction(Access *lhs, Access *rhs){
				return (uint64_t)(lhs->access_record_) < (uint64_t)(rhs->access_record_);
			}

		public:
			size_t access_count_;
		private:
			Access *accesses_[N];
		};
	}
}

#endif
