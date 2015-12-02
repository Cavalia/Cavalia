#include "MsgSender.h"

MsgSender::MsgSender(const std::string &address, void(*callback)(void*, void*), void *hint, int timeout, int linger, int hwm){
	zmq_address_ = "tcp://" + address;
	context_ = zmq_ctx_new();
	socket_ = zmq_socket(context_, ZMQ_PUSH);
	zmq_setsockopt(socket_, ZMQ_SNDTIMEO, &timeout_, sizeof timeout_);
	zmq_setsockopt(socket_, ZMQ_LINGER, &linger_, sizeof linger_);
	zmq_setsockopt(socket_, ZMQ_SNDHWM, &hwm_, sizeof hwm_);
	callback_ = callback;
	hint_ = hint;
	int rc = zmq_connect(socket_, zmq_address_.c_str());
	assert(rc == 0);
}
MsgSender::~MsgSender(){
	zmq_close(socket_);
	zmq_ctx_destroy(context_);
	socket_ = NULL;
	context_ = NULL;
}

//not in use. when the downstream fails, this function keeps blocking and cannot be interrupted.
int MsgSender::SendMsg(const char *data, const size_t &size){
	assert(callback_ != NULL);
	zmq_msg_init_data(&message_, (char *)data, size, callback_, hint_);
	int rc = zmq_msg_send(&message_, socket_, 0);
	zmq_msg_close(&message_);
	return rc;
}

//in use when sending data to downstreams.the downstream operator may fail at any time.
int MsgSender::SendMsgAsync(const char *data, const size_t &size){
	assert(callback_ != NULL);
	zmq_msg_init_data(&message_, (char *)data, size, callback_, hint_);
	int rc = zmq_msg_send(&message_, socket_, ZMQ_DONTWAIT);
	zmq_msg_close(&message_);
	return rc;
}

//in use when delivering message. the assumption is that the remote node never fails during sending.
int MsgSender::SendMsgCpy(const char *data, const size_t &size){
	zmq_msg_init_size(&message_, size);
	memcpy(zmq_msg_data(&message_), data, size);
	int rc = zmq_msg_send(&message_, socket_, 0);
	zmq_msg_close(&message_);
	return rc;
}

//in use when delivering multipart message.
int MsgSender::SendMsgCpyMore(const char *data, const size_t &size){
	zmq_msg_init_size(&message_, size);
	memcpy(zmq_msg_data(&message_), data, size);
	int rc = zmq_msg_send(&message_, socket_, ZMQ_SNDMORE);
	zmq_msg_close(&message_);
	return rc;
}

//not in use.
int MsgSender::SendMsgCpyAsync(const char *data, const size_t &size){
	zmq_msg_init_size(&message_, size);
	memcpy(zmq_msg_data(&message_), data, size);
	int rc = zmq_msg_send(&message_, socket_, ZMQ_DONTWAIT);
	zmq_msg_close(&message_);
	return rc;
}

int MsgSender::SendMsg(const CharArray &msg){
	assert(callback_ != NULL);
	zmq_msg_init_data(&message_, (char *)msg.char_ptr_, msg.size_, callback_, hint_);
	int rc = zmq_msg_send(&message_, socket_, 0);
	zmq_msg_close(&message_);
	return rc;
}

int MsgSender::SendMsgAsync(const CharArray &msg){
	assert(callback_ != NULL);
	zmq_msg_init_data(&message_, (char *)msg.char_ptr_, msg.size_, callback_, hint_);
	int rc = zmq_msg_send(&message_, socket_, ZMQ_DONTWAIT);
	zmq_msg_close(&message_);
	return rc;
}

int MsgSender::SendMsgCpy(const CharArray &msg){
	zmq_msg_init_size(&message_, msg.size_);
	memcpy(zmq_msg_data(&message_), msg.char_ptr_, msg.size_);
	int rc = zmq_msg_send(&message_, socket_, 0);
	zmq_msg_close(&message_);
	return rc;
}

int MsgSender::SendMsgCpyMore(const CharArray &msg){
	zmq_msg_init_size(&message_, msg.size_);
	memcpy(zmq_msg_data(&message_), msg.char_ptr_, msg.size_);
	int rc = zmq_msg_send(&message_, socket_, ZMQ_SNDMORE);
	zmq_msg_close(&message_);
	return rc;
}

int MsgSender::SendMsgCpyAsync(const CharArray &msg){
	zmq_msg_init_size(&message_, msg.size_);
	memcpy(zmq_msg_data(&message_), msg.char_ptr_, msg.size_);
	int rc = zmq_msg_send(&message_, socket_, ZMQ_DONTWAIT);
	zmq_msg_close(&message_);
	return rc;
}
