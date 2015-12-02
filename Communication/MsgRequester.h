#pragma once
#ifndef __COMMUNICATION_MSG_REQUESTER_H__
#define __COMMUNICATION_MSG_REQUESTER_H__

#include <cassert>
#include <string>
#include <cstring>

#include <zmq.h>
#include <CharArray.h>

class MsgRequester{
public:
	MsgRequester(const std::string &address);
	~MsgRequester();
	void IssueRequest(const char *request, const size_t &request_size, char *&reply, size_t &reply_size);
	void IssueRequest(const CharArray &request_str, CharArray &reply_str);

private:
	MsgRequester(const MsgRequester&);
	MsgRequester& operator=(const MsgRequester&);

private:
	std::string zmq_address_;
	void *context_;
	void *socket_;
};

#endif
