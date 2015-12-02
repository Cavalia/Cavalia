#include "MsgReplyer.h"

MsgReplyer::MsgReplyer(const std::string &port, const int &timeout) : timeout_(timeout){
	zmq_address_ = "tcp://*:" + port;
	context_ = zmq_ctx_new();
	socket_ = zmq_socket(context_, ZMQ_REP);
	zmq_setsockopt(socket_, ZMQ_RCVTIMEO, &timeout_, sizeof timeout_);
}

MsgReplyer::~MsgReplyer(){
	zmq_close(socket_);
	zmq_ctx_destroy(context_);
	socket_ = NULL;
	context_ = NULL;
}

void MsgReplyer::Bind(){
	int rc = zmq_bind(socket_, zmq_address_.c_str());
	assert(rc == 0);
}

int MsgReplyer::ReceiveRequest(char *&request, size_t &size){
	zmq_msg_t message;
	zmq_msg_init(&message);
	int rc = zmq_msg_recv(&message, socket_, 0);
	if (rc == -1){
		return rc;
	}
	size = zmq_msg_size(&message);
	request = new char[size];
	memcpy(request, zmq_msg_data(&message), size);
	zmq_msg_close(&message);
	return rc;
}

int MsgReplyer::RespondRequest(const char *reply, const size_t &size){
	zmq_msg_t message;
	zmq_msg_init_size(&message, size);
	memcpy(zmq_msg_data(&message), reply, size);
	int rc = zmq_msg_send(&message, socket_, 0);
	zmq_msg_close(&message);
	return rc;
}

int MsgReplyer::ReceiveRequest(CharArray &request_str){
	zmq_msg_t message;
	zmq_msg_init(&message);
	int rc = zmq_msg_recv(&message, socket_, 0);
	if (rc == -1){
		return rc;
	}
	request_str.Allocate(zmq_msg_size(&message));
	memcpy(request_str.char_ptr_, zmq_msg_data(&message), request_str.size_);
	zmq_msg_close(&message);
	return rc;
}

int MsgReplyer::RespondRequest(const CharArray &reply_str){
	zmq_msg_t message;
	zmq_msg_init_size(&message, reply_str.size_);
	memcpy(zmq_msg_data(&message), reply_str.char_ptr_, reply_str.size_);
	int rc = zmq_msg_send(&message, socket_, 0);
	zmq_msg_close(&message);
	return rc;
}
