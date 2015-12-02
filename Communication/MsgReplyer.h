#pragma once
#ifndef __COMMUNICATION_MSG_REPLYER_H__
#define __COMMUNICATION_MSG_REPLYER_H__

#include <cassert>
#include <string>
#include <cstring>

#include <zmq.h>
#include <CharArray.h>

#include "MsgMacros.h"

class MsgReplyer{
public:
	MsgReplyer(const std::string &port, const int &timeout = kMsgTimeInf);
	~MsgReplyer();
	void Bind();
	int ReceiveRequest(char *&request, size_t &size);
	int RespondRequest(const char *reply, const size_t &size);
	int ReceiveRequest(CharArray &request_str);
	int RespondRequest(const CharArray &reply_str);

private:
	MsgReplyer(const MsgReplyer&);
	MsgReplyer& operator=(const MsgReplyer&);

private:
	int timeout_;
	std::string zmq_address_;
	void *context_;
	void *socket_;
};

#endif
