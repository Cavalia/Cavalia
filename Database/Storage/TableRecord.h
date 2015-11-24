#pragma once
#ifndef __CAVALIA_DATABASE_TABLE_RECORD_H__
#define __CAVALIA_DATABASE_TABLE_RECORD_H__

#include "SchemaRecord.h"

#if defined(LOCK_WAIT)
#include "../Content/LockWaitContent.h"
#elif defined(LOCK) || defined(OCC) || defined(SILO) || defined(HEALING) || defined(HYBRID)
#include "../Content/LockContent.h"
#elif defined(DBX)
#include "../Content/DbxContent.h"
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
#elif defined(SIOCC)
#include "../Content/SiOccContent.h"
#elif defined(SILOCK)
#include "../Content/SiLockContent.h"
#endif

namespace Cavalia{
	namespace Database{
		struct TableRecord{

#if  defined(SILOCK) || defined(SIOCC) || defined(MVLOCK) || defined(MVOCC) || defined(MVTO) || defined(MVLOCK_WAIT)
			TableRecord(SchemaRecord *record) : record_(record), content_(record->data_ptr_) {}
#elif defined(TO)
			TableRecord(SchemaRecord *record) : record_(record), content_(record->data_ptr_, record->schema_ptr_->GetSchemaSize()) {}
#else
			TableRecord(SchemaRecord *record) : record_(record) {}
#endif
			~TableRecord(){}
			
			SchemaRecord *record_;
			
#if defined(SIOCC)
			SiOccContent content_;
#elif defined(SILOCK)
			SiLockContent content_;
#elif defined(MVOCC)
			MvOccContent content_;
#elif defined(TVLOCK)
			TvLockContent content_;
#elif defined(MVLOCK)
			MvLockContent content_;
#elif defined(MVLOCK_WAIT)
			MvLockWaitContent content_;
#elif defined(MVTO)
			MvToContent content_;
#elif defined(TO)
			ToContent content_;
#elif defined(LOCK) || defined(OCC) || defined(SILO) || defined(HEALING) || defined(HYBRID)
			LockContent content_;
#elif defined(DBX)
			DbxContent content_;
#elif defined(LOCK_WAIT)
			LockWaitContent content_;
#endif
		};
	}
}

#endif