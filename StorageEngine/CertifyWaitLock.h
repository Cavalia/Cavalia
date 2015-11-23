#pragma once
#ifndef __CAVALIA_STORAGE_ENGINE_CERTIFY_WAIT_LOCK_H__
#define __CAVALIA_STORAGE_ENGINE_CERTIFY_WAIT_LOCK_H__

#include <cassert>
#include <SpinLock.h>

namespace Cavalia {
	namespace StorageEngine {
		class CertifyWaitLock {
		public:
			CertifyWaitLock() : read_count_(0), is_writing_(false), is_certifying_(false){}
			~CertifyWaitLock(){}

			void AcquireReadLock() {
				while (1){
					while (is_certifying_ == true);
					spinlock_.Lock();
					if (is_certifying_ == true){
						spinlock_.Unlock();
					}
					else{
						++read_count_;
						spinlock_.Unlock();
						return;
					}
				}
			}

			void AcquireWriteLock() {
				while (1){
					while (is_writing_ == true || is_certifying_ == true);
					spinlock_.Lock();
					if (is_writing_ == true || is_certifying_ == true){
						spinlock_.Unlock();
					}
					else{
						is_writing_ = true;
						spinlock_.Unlock();
						return;
					}
				}
			}

			void AcquireCertifyLock() {
				bool rt = true;
				while (1){
					while (read_count_ != 0);
					spinlock_.Lock();
					if (read_count_ != 0){
						spinlock_.Unlock();
					}
					else{
						is_writing_ = false;
						is_certifying_ = true;
						spinlock_.Unlock();
						return;
					}
				}
			}

			void ReleaseReadLock() {
				spinlock_.Lock();
				assert(read_count_ > 0);
				--read_count_;
				spinlock_.Unlock();
			}

			void ReleaseWriteLock() {
				spinlock_.Lock();
				assert(is_writing_ == true);
				is_writing_ = false;
				spinlock_.Unlock();
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