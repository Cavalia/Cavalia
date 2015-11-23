#pragma once
#ifndef __CAVALIA_STORAGE_ENGINE_ACCESS_H__
#define __CAVALIA_STORAGE_ENGINE_ACCESS_H__

#include <string>
#include <algorithm>
#include "SchemaRecord.h"

namespace Cavalia{
	namespace StorageEngine{
		struct Access{
			Access() : access_record_(NULL), local_record_(NULL){
#if defined(REPAIR)
				table_id_ = SIZE_MAX;
				access_id_ = SIZE_MAX;
				is_locked_ = false;
				is_affected_ = false;
#endif
			}
			AccessType access_type_;
			TableRecord *access_record_;
			SchemaRecord *local_record_;

#if defined(LOCK) || defined(OCC) || defined(SILO) || defined(REPAIR) || defined(MVOCC) || defined(HYBRID) || defined(DBX)
			uint64_t timestamp_;
#endif
#if defined(REPAIR)
			size_t table_id_;
			size_t access_id_;
			bool is_locked_;
			bool is_affected_;

			void CopyToLocal(){
				local_record_->CopyFrom(access_record_->record_);
			}

			char* GetGlobalColumn(const size_t &column_id){
				return access_record_->record_->GetColumn(column_id);
			}

			char* GetLocalColumn(const size_t &column_id){
				return local_record_->GetColumn(column_id);
			}

			void GetLocalColumn(const size_t &column_id, std::string &data){
				local_record_->GetColumn(column_id, data);
			}

			void UpdateLocalColumn(const size_t &column_id, const char* data){
				local_record_->UpdateColumn(column_id, data);
			}
#endif
		};

		template<int N>
		struct AccessList{
			AccessList() : access_count_(0){}

			Access *NewAccess(){
				assert(access_count_ < N);
				Access *ret = &(accesses_[access_count_]);
#if defined(REPAIR)
				ret->is_locked_ = false;
				ret->is_affected_ = false;
#endif
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

		struct Insertion{
			Insertion() : insertion_record_(NULL){}

			TableRecord *insertion_record_;
			SchemaRecord *local_record_;
			size_t table_id_;
			std::string primary_key_;

#if defined(REPAIR)
			void SetColumn(const size_t &column_id, const char *data){
				local_record_->SetColumn(column_id, data);
			}

			void SetColumn(const size_t &column_id, const std::string &data){
				local_record_->SetColumn(column_id, data);
			}
#endif
		};

		template<int N>
		struct InsertionList{
			InsertionList() : insertion_count_(0){}

			Insertion *NewInsertion(){
				assert(insertion_count_ < N);
				Insertion *ret = &(insertions_[insertion_count_]);
				++insertion_count_;
				return ret;
			}

			Insertion *GetInsertion(const size_t &index){
				return &(insertions_[index]);
			}

			void Clear(){
				insertion_count_ = 0;
			}

			Insertion insertions_[N];
			size_t insertion_count_;
		};

		template<int N>
		struct InsertionPtrList {
			InsertionPtrList() : insertion_count_(0) {}

			void Add(Insertion *insertion) {
				assert(insertion_count_ < N);
				insertions_[insertion_count_] = insertion;
				++insertion_count_;
			}

			void Clear() {
				insertion_count_ = 0;
			}

			Insertion *insertions_[N];
			size_t insertion_count_;
		};

	}
}

#endif
