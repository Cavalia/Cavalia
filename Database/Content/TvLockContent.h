#pragma once
#ifndef __CAVALIA_DATABASE_TV_LOCK_CONTENT_H__
#define __CAVALIA_DATABASE_TV_LOCK_CONTENT_H__

#include <cassert>
#include <boost/atomic.hpp>
#include <SpinLock.h>

namespace Cavalia {
	namespace Database {
		class TvLockContent {
		public:
			TvLockContent() : read_count_(0), is_writing_(false), is_certifying_(false){}
			~TvLockContent(){}

			bool AcquireReadLock() {
				bool rt = true;
				spinlock_.Lock();
				if (is_certifying_ == true){
					rt = false;
				}
				else{
					++read_count_;
				}
				spinlock_.Unlock();
				return rt;
			}

			void ReleaseReadLock() {
				spinlock_.Lock();
				assert(read_count_ > 0);
				--read_count_;
				spinlock_.Unlock();
			}

			bool AcquireWriteLock() {
				bool rt = true;
				spinlock_.Lock();
				if (is_writing_ == true || is_certifying_ == true){
					rt = false;
				}
				else{
					is_writing_ = true;
				}
				spinlock_.Unlock();
				return rt;
			}

			void ReleaseWriteLock() {
				spinlock_.Lock();
				assert(is_writing_ == true);
				is_writing_ = false;
				spinlock_.Unlock();
			}

			bool AcquireCertifyLock() {
				bool rt = true;
				spinlock_.Lock();
				assert(is_writing_ == true);
				assert(is_certifying_ == false);
				if (read_count_ != 0){
					rt = false;
				}
				else{
					is_writing_ = false;
					is_certifying_ = true;
				}
				spinlock_.Unlock();
				return rt;
			}

			void ReleaseCertifyLock() {
				spinlock_.Lock();
				assert(is_certifying_ == true);
				is_certifying_ = false;
				spinlock_.Unlock();
			}

		private:
			SpinLock spinlock_;
			size_t read_count_;
			bool is_writing_;
			bool is_certifying_;
		};
	}
}

#endif