#pragma once
#ifndef __CAVALIA_DATABASE_NETWORK_CLIENT_H__
#define __CAVALIA_DATABASE_NETWORK_CLIENT_H__

#include <MsgSender.h>
#include <vector>

namespace Cavalia{
	namespace Database{
		class NetworkClient{
		public:
			NetworkClient(const size_t num_clients, const std::vector<std::string> &addresses){
				addresses_ = addresses;
				senders_ = new MsgSender**[num_clients];
				for (size_t c_id = 0; c_id < num_clients; ++c_id){
					senders_[c_id] = new MsgSender*[addresses.size()];
					for (size_t addr_id = 0; addr_id < addresses.size(); ++addr_id){
						senders_[c_id][addr_id] = new MsgSender(addresses[addr_id]);
					}
				}
			}
			virtual ~NetworkClient(){}

			virtual void Start() = 0;

		private:
			NetworkClient(const NetworkClient&);
			NetworkClient & operator=(const NetworkClient&);

		private:
			std::vector<std::string> addresses_;
			MsgSender ***senders_;
		};
	}
}

#endif
