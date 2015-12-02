#pragma once
#ifndef __COMMUNICATION_MSG_SENDER_H__
#define __COMMUNICATION_MSG_SENDER_H__

#include <cassert>
#include <cstring>
#include <string>

#include <zmq.h>
#include <CharArray.h>

#include "MsgMacros.h"

class MsgSender{
public:
	MsgSender(const std::string &address, void(*callback)(void*, void*) = NULL, void *hint = NULL, int timeout = kMsgTimeInf, int linger = kMsgNoLinger, int hwm = kMsgHWM);
	~MsgSender();
	int SendMsg(const char *data, const size_t &size);
	int SendMsgAsync(const char *data, const size_t &size);
	int SendMsgCpy(const char *data, const size_t &size);
	int SendMsgCpyMore(const char *data, const size_t &size);
	int SendMsgCpyAsync(const char *data, const size_t &size);

	int SendMsg(const CharArray &msg);
	int SendMsgAsync(const CharArray &msg);
	int SendMsgCpy(const CharArray &msg);
	int SendMsgCpyMore(const CharArray &msg);
	int SendMsgCpyAsync(const CharArray &msg);

private:
	MsgSender(const MsgSender&);
	MsgSender& operator=(const MsgSender&);

private:
	int timeout_, linger_, hwm_;
	void(*callback_)(void*, void*);
	void *hint_;
	void *context_;
	void *socket_;
	std::string zmq_address_;
	zmq_msg_t message_;
};

#endif
