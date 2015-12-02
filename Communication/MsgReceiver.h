#pragma once
#ifndef __COMMUNICATION_MSG_RECEIVER_H__
#define __COMMUNICATION_MSG_RECEIVER_H__

#include <cassert>
#include <string>
#include <cstring>

#include <zmq.h>
#include <CharArray.h>

#include "MsgMacros.h"

class MsgReceiver{
public:
	//by default, the receiver will be attached on the port and block.
	MsgReceiver(const std::string &port, const int &timeout = kMsgTimeInf, const int &linger = kMsgNoLinger, const int &hwm = kMsgHWM);
	~MsgReceiver();
	int ReceiveMsg(char *&data, size_t &size);
	int ReceiveMsgAsync(char *&data, size_t &size);
	int ReceiveMsg(CharArray &data);
	int ReceiveMsgAsync(CharArray &data);

private:
	MsgReceiver(const MsgReceiver&);
	MsgReceiver& operator=(const MsgReceiver&);

private:
	int timeout_, linger_, hwm_;
	std::string zmq_address_;
	void *context_;
	void *socket_;
	zmq_msg_t message_;
};

#endif