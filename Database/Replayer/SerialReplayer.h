#pragma once
#ifndef __CAVALIA_DATABASE_SERIAL_REPLAYER_H__
#define __CAVALIA_DATABASE_SERIAL_REPLAYER_H__

#include "TransactionManager.h"
#include "BaseReplayer.h"

namespace Cavalia{
	namespace Database{
		class SerialReplayer : public BaseReplayer{
		public:
			SerialReplayer(const std::string &filename, BaseStorageManager *const storage_manager) : BaseReplayer(filename, storage_manager, 1), transaction_manager_(storage_manager_, &logger_){}
			virtual ~SerialReplayer(){
				for (size_t i = 0; i < procedure_count_; ++i){
					delete procedures_[i];
					procedures_[i] = NULL;
				}
				delete[] procedures_;
				procedures_ = NULL;
			}

		private:
			virtual void ProcessLog(){
				TimeMeasurer timer;
				timer.StartTimer();
				std::string ret;
				std::cout << "log buffer size=" << log_buffer_.size() << std::endl;
				for (auto &log_pair : log_buffer_){
					procedures_[log_pair.first]->Execute(log_pair.second, ret);
				}
				timer.EndTimer();
				std::cout << "execute elapsed time=" << timer.GetElapsedMilliSeconds() << "ms" << std::endl;
			}

			virtual void Register(){
				procedure_count_ = kMaxProcedureNum;
				procedures_ = new StoredProcedure*[procedure_count_];
				for (size_t i = 0; i < procedure_count_; ++i) {
					procedures_[i] = NULL;
				}
				RegisterProcedures();
				for (size_t i = 0; i < procedure_count_; ++i){
					if (procedures_[i] != NULL) {
						procedures_[i]->SetTransactionManager(&transaction_manager_);
						//procedures_[i]->SetAllocator(allocators_[0]);
					}
				}
			}

			virtual void RegisterProcedures() = 0;

		private:
			SerialReplayer(const SerialReplayer &);
			SerialReplayer& operator=(const SerialReplayer &);

		protected:
			TransactionManager transaction_manager_;
			StoredProcedure **procedures_;
			size_t procedure_count_;
		};
	}
}

#endif
