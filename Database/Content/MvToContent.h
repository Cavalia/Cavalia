#pragma once
#ifndef __CAVALIA_DATABASE_MVTO_CONTENT_H__
#define __CAVALIA_DATABASE_MVTO_CONTENT_H__

#include <boost/thread/mutex.hpp>
#include <cstdlib>
#include "../Transaction/GlobalTimestamp.h"
#include "../Meta/MetaTypes.h"
#include "ContentCommon.h"

namespace Cavalia {
	namespace Database {

		class MvToContent {
		public:
			MvToContent(char* data_ptr) : data_ptr_(data_ptr){
				read_request_head_ = NULL;
				write_request_head_ = NULL;
				read_history_head_ = NULL;
				write_history_head_ = NULL;
				read_history_tail_ = NULL;
				write_history_tail_ = NULL;
				read_history_length_ = 0;
				write_history_length_ = 0;
				memset(&spinlock_, 0, sizeof(spinlock_));
			}

			~MvToContent() {
				MvRequestEntry* req, *prev_req;
				req = read_request_head_;
				while (req != NULL) {
					prev_req = req;
					req = req->next_;
					delete prev_req;
					prev_req = NULL;
				}
				req = write_request_head_;
				while (req != NULL) {
					prev_req = req;
					req = req->next_;
					delete prev_req;
					prev_req = NULL;
				}
				MvHistoryEntry* his, *prev_his;
				his = read_history_head_;
				while (his != NULL) {
					prev_his = his;
					his = his->next_;
					delete prev_his;
					prev_his = NULL;
				}
				his = write_history_head_;
				while (his != NULL) {
					prev_his = his;
					his = his->next_;
					delete prev_his;
					prev_his = NULL;
				}
			}

			void RequestReadAccess(const uint64_t &timestamp, char **data, volatile bool *is_ready) {
				spinlock_.lock();
				if (IsReadConflict(timestamp) == true) {
					BufferReadRequest(timestamp, data, is_ready);
				}
				else {
					// read immediately
					MvHistoryEntry* entry = write_history_head_;
					while (entry != NULL && entry->timestamp_ > timestamp) {
						entry = entry->next_;
					}
					if (entry == NULL) {
						*data = data_ptr_;
					}
					else {
						*data = entry->data_ptr_;
					}
					InsertReadHistory(timestamp);
					*is_ready = true;
				}
				// clear garbage here.
				//CollectGarbage();
				spinlock_.unlock();
			}

			bool RequestWriteAccess(const uint64_t &timestamp, char **data) {
				spinlock_.lock();
				bool is_success = true;
				if (IsWriteConflict(timestamp) == true) {
					is_success = false;
				}
				else {
					BufferWriteRequest(timestamp, data);
				}
				spinlock_.unlock();
				return is_success;
			}

			void RequestCommit(const uint64_t &timestamp, char* data_ptr){
				spinlock_.lock();
				MvRequestEntry* entry = DebufferWriteRequest(timestamp);
				InsertWriteHistory(timestamp, data_ptr);
				UpdateBuffer();
				delete entry;
				entry = NULL;
				// clear garbage here.
				//CollectGarbage();
				spinlock_.unlock();
			}

			void RequestAbort(const uint64_t &timestamp) {
				spinlock_.lock();
				MvRequestEntry* entry = DebufferWriteRequest(timestamp);
				UpdateBuffer();
				delete entry;
				entry = NULL;
				spinlock_.unlock();
			}

		private:
			bool IsReadConflict(const uint64_t &timestamp) {
				uint64_t wts = 0; // initialize to minimum
				MvRequestEntry* req = write_request_head_;
				/*while (req != NULL) {
				if (req->timestamp_ < timestamp && req->timestamp_ > wts) {
				// find the largest timestamp that is smaller than the read timestamp
				wts = req->timestamp_;
				}
				req = req->next_;
				}*/
				// since now the req list is sorted
				while (req != NULL && req->timestamp_ >= timestamp){
					req = req->next_;
				}
				if (req == NULL)
					return false;

				wts = req->timestamp_;

				// if there is no timestamp smaller than read timestamp, then there is no conflict
				/*if (wts == 0){
				return false;
				}*/
				// if there's a history timestamp larger than wts and smaller than timestamp, then no conflict.
				MvHistoryEntry* write_his = write_history_head_;
				while (write_his != NULL && write_his->timestamp_ > wts) {
					if (write_his->timestamp_ < timestamp){
						return false;
					}
					write_his = write_his->next_;
				}
				return true;
			}

			bool IsWriteConflict(const uint64_t &timestamp) {
				uint64_t rts = 0;
				MvHistoryEntry* read_his = read_history_head_;
				while (read_his != NULL){
					if (read_his->timestamp_ > timestamp) {
						// find the smallest timestamp which is larger than the write timestamp
						rts = read_his->timestamp_;
					}
					else{
						break;
					}
					read_his = read_his->next_;
				}
				if (rts == 0){
					return false;
				}
				// if there's a history timestamp larger than timestamp and smaller than rts, then no conflict.
				MvHistoryEntry* write_his = write_history_head_;
				while (write_his != NULL && write_his->timestamp_ > timestamp) {
					if (write_his->timestamp_ < rts){
						return false;
					}
					write_his = write_his->next_;
				}
				return true;
			}

			void BufferReadRequest(const uint64_t &timestamp, char **data, volatile bool *is_ready) {
				MvRequestEntry* req = new MvRequestEntry();
				req->is_ready_ = is_ready;
				req->data_ = data;
				req->timestamp_ = timestamp;
				/*req->next_ = read_request_head_;
				read_request_head_ = req;*/
				MvRequestEntry* tmp = read_request_head_;
				MvRequestEntry* pre = NULL;
				while (tmp != NULL && timestamp > tmp->timestamp_){
					pre = tmp;
					tmp = tmp->next_;
				}
				if (pre == NULL){
					req->next_ = read_request_head_;
					read_request_head_ = req;
				}
				else{
					req->next_ = tmp;
					pre->next_ = req;
				}
			}

			void BufferWriteRequest(const uint64_t &timestamp, char **data) {
				MvRequestEntry* req = new MvRequestEntry();
				req->data_ = data;
				req->timestamp_ = timestamp;
				MvRequestEntry* tmp = write_request_head_;
				MvRequestEntry* pre = NULL;
				while (tmp != NULL && timestamp > tmp->timestamp_){
					pre = tmp;
					tmp = tmp->next_;
				}
				if (pre == NULL){
					req->next_ = write_request_head_;
					write_request_head_ = req;
				}
				else{
					req->next_ = tmp;
					pre->next_ = req;
				}
				/*req->next_ = write_request_head_;
				write_request_head_ = req;
				assert(write_request_head_ != NULL);*/
			}

			uint64_t GetMinWriteTimestamp() const {
				uint64_t min_ts = INT64_MAX;
				MvRequestEntry *write_entry = write_request_head_;
				while (write_entry != NULL){
					if (write_entry->timestamp_ < min_ts){
						min_ts = write_entry->timestamp_;
					}
					write_entry = write_entry->next_;
				}
				return min_ts;
			}

			MvRequestEntry* DebufferReadRequest() {
				uint64_t min_ts = GetMinWriteTimestamp();
				MvRequestEntry* ret_entry = NULL;
				MvRequestEntry* tmp_entry = read_request_head_;
				MvRequestEntry* prev_entry = NULL;
				// when no write req, return all the reads
				if (min_ts == INT64_MAX) {
					ret_entry = read_request_head_;
					read_request_head_ = NULL;
					return ret_entry;
				}
				while (tmp_entry != NULL && tmp_entry->timestamp_ <= min_ts){
					prev_entry = tmp_entry;
					tmp_entry = tmp_entry->next_;
				}
				if (prev_entry != NULL){
					ret_entry = read_request_head_;
					prev_entry->next_ = NULL;
					read_request_head_ = tmp_entry;
				}
				/*while (tmp_entry != NULL) {
				//assert(tmp_entry->timestamp_ != min_ts);
				// a read would have the same ts as a write if they are in the same read write operation
				if (tmp_entry->timestamp_ <= min_ts) {
				// if not first entry.
				if (prev_entry != NULL) {
				prev_entry->next_ = tmp_entry->next_;
				}
				else{
				read_request_head_ = tmp_entry->next_;
				}
				// add tmp_entry into return list.
				tmp_entry->next_ = ret_entry;
				ret_entry = tmp_entry;
				// if not first entry.
				if (prev_entry != NULL) {
				tmp_entry = prev_entry->next_;
				}
				else{
				tmp_entry = read_request_head_;
				}
				}
				else {
				prev_entry = tmp_entry;
				tmp_entry = tmp_entry->next_;
				}
				}*/

				return ret_entry;
			}

			MvRequestEntry* DebufferWriteRequest(const uint64_t &timestamp) {
				MvRequestEntry* ret_entry = write_request_head_;
				MvRequestEntry* prev_entry = NULL;
				while (ret_entry != NULL && ret_entry->timestamp_ != timestamp) {
					prev_entry = ret_entry;
					ret_entry = ret_entry->next_;
				}
				// we must find exactly one matching request.
				assert(ret_entry != NULL);
				if (prev_entry != NULL){
					prev_entry->next_ = ret_entry->next_;
				}
				else{
					write_request_head_ = ret_entry->next_;
				}
				ret_entry->next_ = NULL;
				return ret_entry;
			}

			// when a write commit, process all reads that wait for this write
			void UpdateBuffer() {
				MvRequestEntry* read_list = DebufferReadRequest();
				if (read_list == NULL){
					return;
				}
				MvRequestEntry *read_entry = read_list;
				while (read_entry != NULL) {
					// find the version from history
					MvHistoryEntry* entry = write_history_head_;
					// a read cannot see a write with the same ts, though this situation cannot exist
					while (entry != NULL && entry->timestamp_ >= read_entry->timestamp_) {
						entry = entry->next_;
					}
					if (entry == NULL) {
						*(read_entry->data_) = data_ptr_;
					}
					else {
						*(read_entry->data_) = entry->data_ptr_;
					}
					InsertReadHistory(read_entry->timestamp_);
					*(read_entry->is_ready_) = true;
					MvRequestEntry *tmp_entry = read_entry;
					read_entry = read_entry->next_;
					delete tmp_entry;
					tmp_entry = NULL;
				}
			}

			void InsertReadHistory(const uint64_t &timestamp) {
				MvHistoryEntry* new_entry = new MvHistoryEntry();
				new_entry->timestamp_ = timestamp;

				++read_history_length_;
				MvHistoryEntry* his = read_history_head_;
				while (his != NULL && timestamp < his->timestamp_) {
					his = his->next_;
				}
				if (his != NULL) {
					new_entry->prev_ = his->prev_;
					new_entry->next_ = his;
					if (his->prev_ == NULL){
						read_history_head_ = new_entry;
					}
					else {
						his->prev_->next_ = new_entry;
					}
					his->prev_ = new_entry;
				}
				else {
					read_history_head_ = read_history_tail_ = new_entry;
				}
			}

			void InsertWriteHistory(const uint64_t &timestamp, char* data_ptr) {
				MvHistoryEntry* new_entry = new MvHistoryEntry();
				new_entry->timestamp_ = timestamp;
				new_entry->data_ptr_ = data_ptr;

				++write_history_length_;
				MvHistoryEntry* his = write_history_head_;
				while (his != NULL && timestamp < his->timestamp_) {
					his = his->next_;
				}
				if (his != NULL) {
					new_entry->prev_ = his->prev_;
					new_entry->next_ = his;
					if (his->prev_ == NULL){
						write_history_head_ = new_entry;
					}
					else {
						his->prev_->next_ = new_entry;
					}
					his->prev_ = new_entry;
				}
				else {
					write_history_head_ = write_history_tail_ = new_entry;
				}
			}

			void CollectGarbage(){
				if (read_history_length_ > kRecycleLength * 1000){
					uint64_t min_thread_ts = GlobalTimestamp::GetMinTimestamp();
					ClearReadHistory(min_thread_ts);
				}
				if (write_history_length_ > kRecycleLength){
					uint64_t min_thread_ts = GlobalTimestamp::GetMinTimestamp();
					ClearWriteHistory(min_thread_ts);
				}
			}

			void ClearReadHistory(const uint64_t &timestamp) {
				MvHistoryEntry* his_entry = read_history_tail_;
				while (his_entry != NULL && his_entry->timestamp_ < timestamp) {
					MvHistoryEntry* tmp_entry = his_entry;
					his_entry = his_entry->prev_;
					delete tmp_entry;
					tmp_entry = NULL;
					--read_history_length_;
				}
				read_history_tail_ = his_entry;
				if (read_history_tail_ != NULL){
					read_history_tail_->next_ = NULL;
				}
				else{
					read_history_head_ = NULL;
				}
			}

			void ClearWriteHistory(const uint64_t &timestamp) {
				MvHistoryEntry* his_entry = write_history_tail_;
				while (his_entry != NULL && his_entry->prev_ != NULL && his_entry->prev_->timestamp_ < timestamp) {
					MvHistoryEntry* tmp_entry = his_entry;
					his_entry = his_entry->prev_;
					delete tmp_entry;
					tmp_entry = NULL;
					--write_history_length_;
				}
				write_history_tail_ = his_entry;
				if (write_history_tail_ != NULL){
					write_history_tail_->next_ = NULL;
				}
			}

		private:
			char* data_ptr_;

			// read and write requests may not be sorted
			MvRequestEntry* read_request_head_;
			MvRequestEntry* write_request_head_;
			// read and write requests must be sorted
			MvHistoryEntry* read_history_head_;
			MvHistoryEntry* write_history_head_;
			MvHistoryEntry* read_history_tail_;
			MvHistoryEntry* write_history_tail_;
			size_t read_history_length_;
			size_t write_history_length_;

			boost::detail::spinlock spinlock_;
		};
	}
}

#endif
