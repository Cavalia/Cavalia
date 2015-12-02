#include "MsgReceiver.h"

MsgReceiver::MsgReceiver(const std::string &port, const int &timeout, const int &linger, const int &hwm) : timeout_(timeout), linger_(linger), hwm_(hwm){
	zmq_address_ = "tcp://*:" + port;
	context_ = zmq_ctx_new();
	socket_ = zmq_socket(context_, ZMQ_PULL);
	zmq_setsockopt(socket_, ZMQ_RCVTIMEO, &timeout_, sizeof timeout_);
	zmq_setsockopt(socket_, ZMQ_LINGER, &linger_, sizeof linger_);
	zmq_setsockopt(socket_, ZMQ_RCVHWM, &hwm_, sizeof hwm_);
	int rc = zmq_bind(socket_, zmq_address_.c_str());
	assert(rc == 0);
}

MsgReceiver::~MsgReceiver(){
	zmq_close(socket_);
	zmq_ctx_destroy(context_);
	socket_ = NULL;
	context_ = NULL;
}

int MsgReceiver::ReceiveMsg(char *&data, size_t &size){
	zmq_msg_init(&message_);
	int rc = zmq_msg_recv(&message_, socket_, 0);
	if (rc == -1){ return rc; }
	size = zmq_msg_size(&message_);
	data = new char[size];
	memcpy(data, zmq_msg_data(&message_), size);
	zmq_msg_close(&message_);
	return rc;
}

int MsgReceiver::ReceiveMsgAsync(char *&data, size_t &size){
	zmq_msg_init(&message_);
	int rc = zmq_msg_recv(&message_, socket_, ZMQ_DONTWAIT);
	if (rc == -1){ return rc; }
	size = zmq_msg_size(&message_);
	data = new char[size];
	memcpy(data, zmq_msg_data(&message_), size);
	zmq_msg_close(&message_);
	return rc;
}

int MsgReceiver::ReceiveMsg(CharArray &msg){
	zmq_msg_init(&message_);
	int rc = zmq_msg_recv(&message_, socket_, 0);
	if (rc == -1){ return rc; }
	msg.Allocate(zmq_msg_size(&message_));
	memcpy(msg.char_ptr_, zmq_msg_data(&message_), msg.size_);
	zmq_msg_close(&message_);
	return rc;
}

int MsgReceiver::ReceiveMsgAsync(CharArray &msg){
	zmq_msg_init(&message_);
	int rc = zmq_msg_recv(&message_, socket_, ZMQ_DONTWAIT);
	if (rc == -1){ return rc; }
	msg.Allocate(zmq_msg_size(&message_));
	memcpy(msg.char_ptr_, zmq_msg_data(&message_), msg.size_);
	zmq_msg_close(&message_);
	return rc;
}
