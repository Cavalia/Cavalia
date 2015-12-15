#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_TPCC_PARAMS_H__
#define __CAVALIA_TPCC_BENCHMARK_TPCC_PARAMS_H__

#include <unordered_map>

#include <Transaction/TxnParam.h>
#include "TpccInformation.h"

namespace Cavalia{
	namespace Benchmark{
		namespace Tpcc{
			using namespace Cavalia::Database;
			class DeliveryParam : public TxnParam{
			public:
				DeliveryParam(){
					type_ = DELIVERY;
				}
				virtual ~DeliveryParam(){}

				virtual uint64_t GetHashCode() const{
					return -1;
				}

				virtual void Serialize(CharArray& serial_str) const {
					serial_str.Allocate(sizeof(int)+sizeof(int)+sizeof(int64_t));
					serial_str.Memcpy(0, reinterpret_cast<const char*>(&w_id_), sizeof(int));
					serial_str.Memcpy(sizeof(int), reinterpret_cast<const char*>(&o_carrier_id_), sizeof(int));
					serial_str.Memcpy(sizeof(int)+sizeof(int), reinterpret_cast<const char*>(&ol_delivery_d_), sizeof(int64_t));
				}
				virtual void Serialize(char *buffer, size_t &buffer_size) const {
					buffer_size = sizeof(int) + sizeof(int) + sizeof(int64_t);
					memcpy(buffer, reinterpret_cast<const char*>(&w_id_), sizeof(int));
					memcpy(buffer + sizeof(int), reinterpret_cast<const char*>(&o_carrier_id_), sizeof(int));
					memcpy(buffer + sizeof(int) + sizeof(int), reinterpret_cast<const char*>(&ol_delivery_d_), sizeof(int64_t));
				}
				virtual void Deserialize(const CharArray& serial_str) {
					memcpy(reinterpret_cast<char*>(&w_id_), serial_str.char_ptr_, sizeof(int));
					memcpy(reinterpret_cast<char*>(&o_carrier_id_), serial_str.char_ptr_ + sizeof(int), sizeof(int));
					memcpy(reinterpret_cast<char*>(&ol_delivery_d_), serial_str.char_ptr_ + sizeof(int)+sizeof(int), sizeof(int64_t));
				}

			public:
				int w_id_;
				int o_carrier_id_;
				int64_t ol_delivery_d_;
//#if defined(SLICE)
				// additional parameters
				int no_o_ids_[DISTRICTS_PER_WAREHOUSE];
				double sums_[DISTRICTS_PER_WAREHOUSE];
				int c_ids_[DISTRICTS_PER_WAREHOUSE];
//#endif
			};

			class NewOrderParam : public TxnParam{
			public:
				NewOrderParam(){
					type_ = NEW_ORDER;
				}
				virtual ~NewOrderParam(){}

				virtual uint64_t GetHashCode() const{
					return -1;
				}

				virtual void Serialize(CharArray& serial_str) const {
					serial_str.Allocate(sizeof(int)* 3 + sizeof(int64_t)+sizeof(size_t)+sizeof(int)* 45);
					size_t offset = 0;
					serial_str.Memcpy(offset, reinterpret_cast<const char*>(&w_id_), sizeof(int));
					offset += sizeof(int);
					serial_str.Memcpy(offset, reinterpret_cast<const char*>(&d_id_), sizeof(int));
					offset += sizeof(int);
					serial_str.Memcpy(offset, reinterpret_cast<const char*>(&c_id_), sizeof(int));
					offset += sizeof(int);
					serial_str.Memcpy(offset, reinterpret_cast<const char*>(&o_entry_d_), sizeof(int64_t));
					offset += sizeof(int64_t);
					serial_str.Memcpy(offset, reinterpret_cast<const char*>(&ol_cnt_), sizeof(size_t));
					offset += sizeof(size_t);
					serial_str.Memcpy(offset, reinterpret_cast<const char*>(i_ids_), sizeof(int)* 15);
					offset += sizeof(int)* 15;
					serial_str.Memcpy(offset, reinterpret_cast<const char*>(i_w_ids_), sizeof(int)* 15);
					offset += sizeof(int)* 15;
					serial_str.Memcpy(offset, reinterpret_cast<const char*>(i_qtys_), sizeof(int)* 15);
				}
				virtual void Serialize(char *buffer, size_t &buffer_size) const {
					buffer_size = sizeof(int) * 3 + sizeof(int64_t) + sizeof(size_t) + sizeof(int) * 45;
					size_t offset = 0;
					memcpy(buffer + offset, reinterpret_cast<const char*>(&w_id_), sizeof(int));
					offset += sizeof(int);
					memcpy(buffer + offset, reinterpret_cast<const char*>(&d_id_), sizeof(int));
					offset += sizeof(int);
					memcpy(buffer + offset, reinterpret_cast<const char*>(&c_id_), sizeof(int));
					offset += sizeof(int);
					memcpy(buffer + offset, reinterpret_cast<const char*>(&o_entry_d_), sizeof(int64_t));
					offset += sizeof(int64_t);
					memcpy(buffer + offset, reinterpret_cast<const char*>(&ol_cnt_), sizeof(size_t));
					offset += sizeof(size_t);
					memcpy(buffer + offset, reinterpret_cast<const char*>(i_ids_), sizeof(int) * 15);
					offset += sizeof(int) * 15;
					memcpy(buffer + offset, reinterpret_cast<const char*>(i_w_ids_), sizeof(int) * 15);
					offset += sizeof(int) * 15;
					memcpy(buffer + offset, reinterpret_cast<const char*>(i_qtys_), sizeof(int) * 15);
				}
				virtual void Deserialize(const CharArray& serial_str) {
					size_t offset = 0;
					memcpy(reinterpret_cast<char*>(&w_id_), serial_str.char_ptr_ + offset, sizeof(int));
					offset += sizeof(int);
					memcpy(reinterpret_cast<char*>(&d_id_), serial_str.char_ptr_ + offset, sizeof(int));
					offset += sizeof(int);
					memcpy(reinterpret_cast<char*>(&c_id_), serial_str.char_ptr_ + offset, sizeof(int));
					offset += sizeof(int);
					memcpy(reinterpret_cast<char*>(&o_entry_d_), serial_str.char_ptr_ + offset, sizeof(int64_t));
					offset += sizeof(int64_t);
					memcpy(reinterpret_cast<char*>(&ol_cnt_), serial_str.char_ptr_ + offset, sizeof(size_t));
					offset += sizeof(size_t);
					memcpy(reinterpret_cast<char*>(i_ids_), serial_str.char_ptr_ + offset, sizeof(int)* 15);
					offset += sizeof(int)* 15;
					memcpy(reinterpret_cast<char*>(i_w_ids_), serial_str.char_ptr_ + offset, sizeof(int)* 15);
					offset += sizeof(int)* 15;
					memcpy(reinterpret_cast<char*>(i_qtys_), serial_str.char_ptr_ + offset, sizeof(int)* 15);
				}

			public:
				int w_id_;
				int d_id_;
				int c_id_;
				int64_t o_entry_d_;
				size_t ol_cnt_;
				int i_ids_[15];
				int i_w_ids_[15];
				int i_qtys_[15];
//#if defined(SLICE)
				// additional parameters
				int next_o_id_;
				std::string s_dists_[15];
				double ol_amounts_[15];
//#endif
			};

			class PaymentParam : public TxnParam{
			public:
				PaymentParam(){
					type_ = PAYMENT;
				}
				virtual ~PaymentParam(){}

				virtual uint64_t GetHashCode() const{
					return -1;
				}

				virtual void Serialize(CharArray& serial_str) const {
					serial_str.Allocate(sizeof(int)* 5 + sizeof(double)+sizeof(int64_t));
					size_t offset = 0;
					serial_str.Memcpy(offset, reinterpret_cast<const char*>(&w_id_), sizeof(int));
					offset += sizeof(int);
					serial_str.Memcpy(offset, reinterpret_cast<const char*>(&d_id_), sizeof(int));
					offset += sizeof(int);
					serial_str.Memcpy(offset, reinterpret_cast<const char*>(&h_amount_), sizeof(double));
					offset += sizeof(double);
					serial_str.Memcpy(offset, reinterpret_cast<const char*>(&c_w_id_), sizeof(int));
					offset += sizeof(int);
					serial_str.Memcpy(offset, reinterpret_cast<const char*>(&c_d_id_), sizeof(int));
					offset += sizeof(int);
					serial_str.Memcpy(offset, reinterpret_cast<const char*>(&c_id_), sizeof(int));
					offset += sizeof(int);
					serial_str.Memcpy(offset, reinterpret_cast<const char*>(&h_date_), sizeof(int64_t));
				}
				virtual void Serialize(char *buffer, size_t &buffer_size) const {
					buffer_size = sizeof(int) * 5 + sizeof(double) + sizeof(int64_t);
					size_t offset = 0;
					memcpy(buffer + offset, reinterpret_cast<const char*>(&w_id_), sizeof(int));
					offset += sizeof(int);
					memcpy(buffer + offset, reinterpret_cast<const char*>(&d_id_), sizeof(int));
					offset += sizeof(int);
					memcpy(buffer + offset, reinterpret_cast<const char*>(&h_amount_), sizeof(double));
					offset += sizeof(double);
					memcpy(buffer + offset, reinterpret_cast<const char*>(&c_w_id_), sizeof(int));
					offset += sizeof(int);
					memcpy(buffer + offset, reinterpret_cast<const char*>(&c_d_id_), sizeof(int));
					offset += sizeof(int);
					memcpy(buffer + offset, reinterpret_cast<const char*>(&c_id_), sizeof(int));
					offset += sizeof(int);
					memcpy(buffer + offset, reinterpret_cast<const char*>(&h_date_), sizeof(int64_t));
				}
				virtual void Deserialize(const CharArray& serial_str) {
					size_t offset = 0;
					memcpy(reinterpret_cast<char*>(&w_id_), serial_str.char_ptr_ + offset, sizeof(int));
					offset += sizeof(int);
					memcpy(reinterpret_cast<char*>(&d_id_), serial_str.char_ptr_ + offset, sizeof(int));
					offset += sizeof(int);
					memcpy(reinterpret_cast<char*>(&h_amount_), serial_str.char_ptr_ + offset, sizeof(double));
					offset += sizeof(double);
					memcpy(reinterpret_cast<char*>(&c_w_id_), serial_str.char_ptr_ + offset, sizeof(int));
					offset += sizeof(int);
					memcpy(reinterpret_cast<char*>(&c_d_id_), serial_str.char_ptr_ + offset, sizeof(int));
					offset += sizeof(int);
					memcpy(reinterpret_cast<char*>(&c_id_), serial_str.char_ptr_ + offset, sizeof(int));
					offset += sizeof(int);
					memcpy(reinterpret_cast<char*>(&h_date_), serial_str.char_ptr_ + offset, sizeof(int64_t));
				}

			public:
				int w_id_;
				int d_id_;
				double h_amount_;
				int c_w_id_;
				int c_d_id_;
				int c_id_;
				std::string c_last_;
				int64_t h_date_;
			};

			class OrderStatusParam : public TxnParam{
			public:
				OrderStatusParam(){
					type_ = ORDER_STATUS;
				}
				virtual ~OrderStatusParam(){}

				virtual uint64_t GetHashCode() const{
					return -1;
				}

				virtual void Serialize(CharArray& serial_str) const {
					serial_str.Allocate(sizeof(int)+sizeof(int)+sizeof(int));
					serial_str.Memcpy(0, reinterpret_cast<const char*>(&w_id_), sizeof(int));
					serial_str.Memcpy(sizeof(int), reinterpret_cast<const char*>(&d_id_), sizeof(int));
					serial_str.Memcpy(sizeof(int)+sizeof(int), reinterpret_cast<const char*>(&c_id_), sizeof(int));
				}
				virtual void Serialize(char *buffer, size_t &buffer_size) const {
					buffer_size = sizeof(int) + sizeof(int) + sizeof(int);
					memcpy(buffer, reinterpret_cast<const char*>(&w_id_), sizeof(int));
					memcpy(buffer + sizeof(int), reinterpret_cast<const char*>(&d_id_), sizeof(int));
					memcpy(buffer + sizeof(int) + sizeof(int), reinterpret_cast<const char*>(&c_id_), sizeof(int));
				}
				virtual void Deserialize(const CharArray& serial_str) {
					memcpy(reinterpret_cast<char*>(&w_id_), serial_str.char_ptr_, sizeof(int));
					memcpy(reinterpret_cast<char*>(&d_id_), serial_str.char_ptr_ + sizeof(int), sizeof(int));
					memcpy(reinterpret_cast<char*>(&c_id_), serial_str.char_ptr_ + sizeof(int)+sizeof(int), sizeof(int));
				}

			public:
				int w_id_;
				int d_id_;
				std::string c_last_;
				int c_id_;
			};

			class StockLevelParam : public TxnParam{
			public:
				StockLevelParam(){
					type_ = STOCK_LEVEL;
				}
				virtual ~StockLevelParam(){}

				virtual uint64_t GetHashCode() const{
					return -1;
				}

				virtual void Serialize(CharArray& serial_str) const {
					serial_str.Allocate(sizeof(int)+sizeof(int));
					serial_str.Memcpy(0, reinterpret_cast<const char*>(&w_id_), sizeof(int));
					serial_str.Memcpy(sizeof(int), reinterpret_cast<const char*>(&d_id_), sizeof(int));
				}
				virtual void Serialize(char *buffer, size_t &buffer_size) const {
					buffer_size = sizeof(int) + sizeof(int);
					memcpy(buffer, reinterpret_cast<const char*>(&w_id_), sizeof(int));
					memcpy(buffer + sizeof(int), reinterpret_cast<const char*>(&d_id_), sizeof(int));
				}
				virtual void Deserialize(const CharArray& serial_str) {
					memcpy(reinterpret_cast<char*>(&w_id_), serial_str.char_ptr_, sizeof(int));
					memcpy(reinterpret_cast<char*>(&d_id_), serial_str.char_ptr_ + sizeof(int), sizeof(int));
				}

			public:
				int w_id_;
				int d_id_;
			};
		}
	}
}

#endif