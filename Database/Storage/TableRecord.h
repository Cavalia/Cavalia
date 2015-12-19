#pragma once
#ifndef __CAVALIA_DATABASE_TABLE_RECORD_H__
#define __CAVALIA_DATABASE_TABLE_RECORD_H__

#include "SchemaRecord.h"

#if defined(LOCK_WAIT)
#include "../Content/LockWaitContent.h"
#elif defined(LOCK) || defined(OCC) || defined(SILO) || defined(ST)
#include "../Content/LockContent.h"
#elif defined(SILOCK) || defined(SIOCC)
#include "../Content/SiLockContent.h"
#elif defined(TO)
#include "../Content/ToContent.h"
#elif defined(MVTO)
#include "../Content/MvToContent.h"
#elif defined(TVLOCK)
#include "../Content/TvLockContent.h"
#elif defined(MVLOCK)
#include "../Content/MvLockContent.h"
#elif defined(MVLOCK_WAIT)
#include "../Content/MvLockWaitContent.h"
#elif defined(MVOCC)
#include "../Content/MvOccContent.h"
#elif defined(DBX)
#include "../Content/DbxContent.h"
#elif defined(RTM)
#include "../Content/RtmContent.h"
#elif defined(OCC_RTM) || defined(LOCK_RTM)
#include "../Content/LockRtmContent.h"
#endif

namespace Cavalia{
	namespace Database{
#ifdef __linux__
		struct __attribute__((aligned(64))) TableRecord{
#else
		struct TableRecord{
#endif
#if defined(MVLOCK_WAIT) || defined(MVLOCK) || defined(MVOCC) || defined(MVTO) || defined(SILOCK) || defined(SIOCC)
			TableRecord(SchemaRecord *record) : record_(record), content_(record->data_ptr_) {}
#elif defined(TO)
			TableRecord(SchemaRecord *record) : record_(record), content_(record->data_ptr_, record->schema_ptr_->GetSchemaSize()) {}
#else
			TableRecord(SchemaRecord *record) : record_(record) {}
#endif
			~TableRecord(){}
			
			SchemaRecord *record_;

#if defined(LOCK_WAIT)
			LockWaitContent content_;
#elif defined(LOCK) || defined(OCC) || defined(SILO) || defined(ST)
			LockContent content_;
#elif defined(SILOCK) || defined(SIOCC)
			SiLockContent content_;
#elif defined(TO)
			ToContent content_;
#elif defined(MVTO)
			MvToContent content_;
#elif defined(TVLOCK)
			TvLockContent content_;
#elif defined(MVLOCK)
			MvLockContent content_;
#elif defined(MVLOCK_WAIT)
			MvLockWaitContent content_;
#elif defined(MVOCC)
			MvOccContent content_;
#elif defined(DBX)
			DbxContent content_;
#elif defined(RTM)
			RtmContent content_;
#elif defined(OCC_RTM) || defined(LOCK_RTM)
			LockRtmContent content_;
#endif
		};
	}
}

#endif
