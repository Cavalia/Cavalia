#pragma once
#ifndef __CAVALIA_STORAGE_ENGINE_SERVER_CONTACTOR_H__
#define __CAVALIA_STORAGE_ENGINE_SERVER_CONTACTOR_H__

#include <MsgReplyer.h>
#include <CharArray.h>
#include "StorageManager.h"
#include "BaseLogger.h"
#include "TransactionManager.h"

namespace Cavalia{
	namespace StorageEngine{
		class ServerContactor{
		public:
			ServerContactor(const std::string &port, StorageManager *const storage_manager, BaseLogger *const logger) : replyer_(port){
				replyer_.Bind();
				transaction_manager_ = new TransactionManager(storage_manager, logger);
			}
			~ServerContactor(){
				// TODO: potential memory leak problem.
				//delete transaction_manager_;
				//transaction_manager_ = NULL;
			}

			void Start(){
				while (true){
					CharArray recv_msg;
					replyer_.ReceiveRequest(recv_msg);
					
					recv_msg.Release();
					CharArray reply_msg;
					reply_msg.Allocate(sizeof(int));
					int ret = 1;
					reply_msg.Memcpy(0, (char*)(&ret), sizeof(ret));
					replyer_.RespondRequest(reply_msg);
					reply_msg.Release();
				}
			}


		private:
			ServerContactor(const ServerContactor &);
			ServerContactor& operator=(const ServerContactor &);

		private:
			MsgReplyer replyer_;
			TransactionManager *transaction_manager_;
		};
	}
}

#endif