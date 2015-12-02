#pragma once
#ifndef __CAVALIA_DATABASE_COHORT_H__
#define __CAVALIA_DATABASE_COHORT_H__

#include <MsgReplyer.h>

namespace Cavalia{
	namespace Database{
		class Cohort{
		public:
			Cohort(const std::string &port) : replyer_(port){}
			~Cohort(){}

		private:
			Cohort(const Cohort&);
			Cohort & operator=(const Cohort&);

		private:
			MsgReplyer replyer_;
		};
	}
}

#endif
