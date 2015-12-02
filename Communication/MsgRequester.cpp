#include "MsgRequester.h"

MsgRequester::MsgRequester(const std::string &address){
	zmq_address_ = "tcp://" + address;
	context_ = zmq_ctx_new();
	socket_ = zmq_socket(context_, ZMQ_REQ);
	int rc = zmq_connect(socket_, zmq_address_.c_str());
	assert(rc == 0);
}

MsgRequester::~MsgRequester(){
	zmq_close(socket_);
	zmq_ctx_destroy(context_);
	socket_ = NULL;
	context_ = NULL;
}

void MsgRequester::IssueRequest(const char *request, const size_t &request_size, char *&reply, size_t &reply_size){
	zmq_msg_t request_msg;
	zmq_msg_init_size(&request_msg, request_size);
	memcpy(zmq_msg_data(&request_msg), request, request_size);
	int rc = zmq_msg_send(&request_msg, socket_, 0);
	assert(rc != -1);
	zmq_msg_close(&request_msg);
	zmq_msg_t reply_msg;
	zmq_msg_init(&reply_msg);
	rc = zmq_msg_recv(&reply_msg, socket_, 0);
	assert(rc != -1);
	reply_size = zmq_msg_size(&reply_msg);
	reply = static_cast<char*>(malloc(reply_size));
	memcpy(reply, zmq_msg_data(&reply_msg), reply_size);
	zmq_msg_close(&reply_msg);
}

void MsgRequester::IssueRequest(const CharArray &request_str, CharArray &reply_str){
	zmq_msg_t request_msg;
	zmq_msg_init_size(&request_msg, request_str.size_);
	memcpy(zmq_msg_data(&request_msg), request_str.char_ptr_, request_str.size_);
	int rc = zmq_msg_send(&request_msg, socket_, 0);
	assert(rc != -1);
	zmq_msg_close(&request_msg);
	zmq_msg_t reply_msg;
	zmq_msg_init(&reply_msg);
	rc = zmq_msg_recv(&reply_msg, socket_, 0);
	assert(rc != -1);
	reply_str.Allocate(zmq_msg_size(&reply_msg));
	memcpy(reply_str.char_ptr_, zmq_msg_data(&reply_msg), reply_str.size_);
	zmq_msg_close(&reply_msg);
}
