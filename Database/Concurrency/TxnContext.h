#pragma once
#ifndef __CAVALIA_DATABASE_TXN_CONTEXT_H__
#define __CAVALIA_DATABASE_TXN_CONTEXT_H__

namespace Cavalia{
	namespace Database{
		struct ExeContext{
			ExeContext() : is_adhoc_(false), is_retry_(false){}
			bool is_adhoc_;
			bool is_retry_;
		};

		struct TxnContext{
			TxnContext() : is_read_only_(false), is_dependent_(false), is_adhoc_(false), is_retry_(false){}
			void PassContext(const ExeContext &context){
				is_adhoc_ = context.is_adhoc_;
				is_retry_ = context.is_retry_;
			}
			size_t txn_type_;
			bool is_read_only_;
			bool is_dependent_;
			bool is_adhoc_;
			bool is_retry_;
		};
	}
}

#endif
