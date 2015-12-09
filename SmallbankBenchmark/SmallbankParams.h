#pragma once
#ifndef __CAVALIA_SMALLBANK_BENCHMARK_SMALLBANK_PARAMS_H__
#define __CAVALIA_SMALLBANK_BENCHMARK_SMALLBANK_PARAMS_H__

#include <Transaction/TxnParam.h>
#include "SmallbankRecords.h"
#include "SmallbankMeta.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Smallbank{
			using namespace Cavalia::Database;
			class AmalgamateParam : public TxnParam{
			public:
				AmalgamateParam(){
					type_ = AMALGAMATE;
				}
				virtual ~AmalgamateParam(){}

				virtual uint64_t GetHashCode() const{
					return -1;
				}
				virtual void Serialize(CharArray& serial_str) const {
					serial_str.Allocate(sizeof(int64_t) * 2);
					serial_str.Memcpy(0, reinterpret_cast<const char*>(&custid_0_), sizeof(int64_t));
					serial_str.Memcpy(sizeof(int64_t), reinterpret_cast<const char*>(&custid_1_), sizeof(int64_t));
				}
				virtual void Serialize(char *buffer, size_t &buffer_size) const {
					buffer_size = sizeof(int64_t) * 2;
					memcpy(buffer, reinterpret_cast<const char*>(&custid_0_), sizeof(int64_t));
					memcpy(buffer + sizeof(int64_t), reinterpret_cast<const char*>(&custid_1_), sizeof(int64_t));
				}
				virtual void Deserialize(const CharArray& serial_str) {
					memcpy(reinterpret_cast<char*>(&custid_0_), serial_str.char_ptr_, sizeof(int64_t));
					memcpy(reinterpret_cast<char*>(&custid_1_), serial_str.char_ptr_ + sizeof(int64_t), sizeof(int64_t));
				}
			public:
				int64_t custid_0_;
				int64_t custid_1_;
			};

			class BalanceParam : public TxnParam{
			public:
				BalanceParam(){
					type_ = BALANCE;
				}
				virtual ~BalanceParam(){}

				virtual uint64_t GetHashCode() const{
					return -1;
				}
				virtual void Serialize(CharArray& serial_str) const {
					serial_str.Allocate(sizeof(int64_t));
					serial_str.Memcpy(0, reinterpret_cast<const char*>(&custid_), sizeof(int64_t));
				}
				virtual void Serialize(char *buffer, size_t &buffer_size) const {
					buffer_size = sizeof(int64_t);
					memcpy(buffer, reinterpret_cast<const char*>(&custid_), sizeof(int64_t));
				}
				virtual void Deserialize(const CharArray& serial_str) {
					memcpy(reinterpret_cast<char*>(&custid_), serial_str.char_ptr_, sizeof(int64_t));
				}
			public:
				int64_t custid_;
			};

			class DepositCheckingParam : public TxnParam{
			public:
				DepositCheckingParam(){
					type_ = DEPOSIT_CHECKING;
				}
				virtual ~DepositCheckingParam(){}

				virtual uint64_t GetHashCode() const{
					return -1;
				}
				virtual void Serialize(CharArray& serial_str) const {
					serial_str.Allocate(sizeof(int64_t) + sizeof(float));
					serial_str.Memcpy(0, reinterpret_cast<const char*>(&custid_), sizeof(int64_t));
					serial_str.Memcpy(sizeof(int64_t), reinterpret_cast<const char*>(&amount_), sizeof(float));
				}
				virtual void Serialize(char *buffer, size_t &buffer_size) const {
					buffer_size = sizeof(int64_t) + sizeof(float);
					memcpy(buffer, reinterpret_cast<const char*>(&custid_), sizeof(int64_t));
					memcpy(buffer + sizeof(int64_t), reinterpret_cast<const char*>(&amount_), sizeof(float));
				}
				virtual void Deserialize(const CharArray& serial_str) {
					memcpy(reinterpret_cast<char*>(&custid_), serial_str.char_ptr_, sizeof(int64_t));
					memcpy(reinterpret_cast<char*>(&amount_), serial_str.char_ptr_ + sizeof(int64_t), sizeof(float));
				}
			public:
				int64_t custid_;
				float amount_;
			};

			class SendPaymentParam : public TxnParam{
			public:
				SendPaymentParam(){
					type_ = SEND_PAYMENT;
				}
				virtual ~SendPaymentParam(){}

				virtual uint64_t GetHashCode() const{
					return -1;
				}
				virtual void Serialize(CharArray& serial_str) const {
					serial_str.Allocate(sizeof(int64_t) * 2 + sizeof(float));
					size_t offset = 0;
					serial_str.Memcpy(offset, reinterpret_cast<const char*>(&custid_0_), sizeof(int64_t));
					offset += sizeof(int64_t);
					serial_str.Memcpy(offset, reinterpret_cast<const char*>(&custid_1_), sizeof(int64_t));
					offset += sizeof(int64_t);
					serial_str.Memcpy(offset, reinterpret_cast<const char*>(&amount_), sizeof(float));
				}
				virtual void Serialize(char *buffer, size_t &buffer_size) const {
					buffer_size = sizeof(int64_t) * 2 + sizeof(float);
					size_t offset = 0;
					memcpy(buffer + offset, reinterpret_cast<const char*>(&custid_0_), sizeof(int64_t));
					offset += sizeof(int64_t);
					memcpy(buffer + offset, reinterpret_cast<const char*>(&custid_1_), sizeof(int64_t));
					offset += sizeof(int64_t);
					memcpy(buffer + offset, reinterpret_cast<const char*>(&amount_), sizeof(float));
				}
				virtual void Deserialize(const CharArray& serial_str) {
					size_t offset = 0;
					memcpy(reinterpret_cast<char*>(&custid_0_), serial_str.char_ptr_ + offset, sizeof(int64_t));
					offset += sizeof(int64_t);
					memcpy(reinterpret_cast<char*>(&custid_1_), serial_str.char_ptr_ + offset, sizeof(int64_t));
					offset += sizeof(int64_t);
					memcpy(reinterpret_cast<char*>(&amount_), serial_str.char_ptr_ + offset, sizeof(float));
				}
			public:
				int64_t custid_0_; // send account
				int64_t custid_1_; //dest account
				float amount_;
			};

			class TransactSavingsParam : public TxnParam{
			public:
				TransactSavingsParam(){
					type_ = TRANSACT_SAVINGS;
				}
				virtual ~TransactSavingsParam(){}

				virtual uint64_t GetHashCode() const{
					return -1;
				}
				virtual void Serialize(CharArray& serial_str) const {
					serial_str.Allocate(sizeof(int64_t)+sizeof(float));
					size_t offset = 0;
					serial_str.Memcpy(offset, reinterpret_cast<const char*>(&custid_), sizeof(int64_t));
					offset += sizeof(int64_t);
					serial_str.Memcpy(offset, reinterpret_cast<const char*>(&amount_), sizeof(float));
				}
				virtual void Serialize(char *buffer, size_t &buffer_size) const {
					buffer_size = sizeof(int64_t) + sizeof(float);
					size_t offset = 0;
					memcpy(buffer + offset, reinterpret_cast<const char*>(&custid_), sizeof(int64_t));
					offset += sizeof(int64_t);
					memcpy(buffer + offset, reinterpret_cast<const char*>(&amount_), sizeof(float));
				}
				virtual void Deserialize(const CharArray& serial_str) {
					size_t offset = 0;
					memcpy(reinterpret_cast<char*>(&custid_), serial_str.char_ptr_ + offset, sizeof(int64_t));
					offset += sizeof(int64_t);
					memcpy(reinterpret_cast<char*>(&amount_), serial_str.char_ptr_ + offset, sizeof(float));
				}
			public:
				int64_t custid_;
				float amount_;
			};

			class WriteCheckParam : public TxnParam{
			public:
				 WriteCheckParam(){
					 type_ = WRITE_CHECK;
				 }
				virtual ~WriteCheckParam(){}

				virtual uint64_t GetHashCode() const{
					return -1;
				}
				virtual void Serialize(CharArray& serial_str) const {
					serial_str.Allocate(sizeof(int64_t)+sizeof(float));
					size_t offset = 0;
					serial_str.Memcpy(offset, reinterpret_cast<const char*>(&custid_), sizeof(int64_t));
					offset += sizeof(int64_t);
					serial_str.Memcpy(offset, reinterpret_cast<const char*>(&amount_), sizeof(float));
				}
				virtual void Serialize(char *buffer, size_t &buffer_size) const {
					buffer_size = sizeof(int64_t) + sizeof(float);
					size_t offset = 0;
					memcpy(buffer + offset, reinterpret_cast<const char*>(&custid_), sizeof(int64_t));
					offset += sizeof(int64_t);
					memcpy(buffer + offset, reinterpret_cast<const char*>(&amount_), sizeof(float));
				}
				virtual void Deserialize(const CharArray& serial_str) {
					size_t offset = 0;
					memcpy(reinterpret_cast<char*>(&custid_), serial_str.char_ptr_ + offset, sizeof(int64_t));
					offset += sizeof(int64_t);
					memcpy(reinterpret_cast<char*>(&amount_), serial_str.char_ptr_ + offset, sizeof(float));
				}
			public:
				int64_t custid_;
				float amount_;
			};
		}
	}
}

#endif