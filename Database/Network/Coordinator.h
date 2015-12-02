#pragma once
#ifndef __CAVALIA_DATABASE_COORDINATOR_H__
#define __CAVALIA_DATABASE_COORDINATOR_H__

#include <MsgRequester.h>

namespace Cavalia{
	namespace Database{
		class Coordinator{
		public:
			Coordinator(const std::string &address) : requester_(address){}
			~Coordinator(){}

		private:
			Coordinator(const Coordinator&);
			Coordinator & operator=(const Coordinator&);

		private:
			MsgRequester requester_;
		};
	}
}

#endif
