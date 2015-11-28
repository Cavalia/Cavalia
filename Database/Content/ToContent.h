#pragma once
#ifndef __CAVALIA_DATABASE_TO_CONTENT_H__
#define __CAVALIA_DATABASE_TO_CONTENT_H__

#include <SpinLock.h>
#include <vector>
#include "ContentCommon.h"

namespace Cavalia{
	namespace Database{

		class ToContent{
		public:
			ToContent(char *data_ptr, const size_t &data_size) : data_ptr_(data_ptr), data_size_(data_size){
				read_ts_ = 0;
				write_ts_ = 0;
				min_read_ts_ = INT64_MAX;
				min_write_ts_ = INT64_MAX;
				read_requests_head_ = NULL;
				write_requests_head_ = NULL;
				commit_requests_head_ = NULL;
			}

			bool RequestReadAccess(const uint64_t &timestamp, char **data, volatile bool* is_ready){
				bool is_success = true;
				spinlock_.Lock();
				// if read ts is smaller than write ts that has been committed, then abort.
				if (timestamp < write_ts_){
					is_success = false;
				}
				// if read ts is larger than minimum write ts in the queue, then buffer.
				else if (timestamp > min_write_ts_){
					// put into queue.
					BufferReadRequest(timestamp, data, is_ready);
				}
				// if read ts is smaller than or equal to minimum write ts in the queue, then directly return.
				else{
					// copy data here. protected by mutex.
					// data has already been allocated.
					memcpy(*data, data_ptr_, data_size_);
					// directly read.
					if (read_ts_ < timestamp) {
						read_ts_ = timestamp;
					}
					*is_ready = true;
				}
				spinlock_.Unlock();
				return is_success;
			}

			bool RequestWriteAccess(const uint64_t &timestamp, char **data){
				bool is_success = true;
				spinlock_.Lock();
				// if write ts is smaller than read ts that has been issued, than abort.
				if (timestamp < read_ts_){
					is_success = false;
				}
				// is write ts is larger than or equal to read ts that has been issued, than buffer.
				else{
					// thomas write rule.
					BufferWriteRequest(timestamp, data);
				}
				spinlock_.Unlock();
				return is_success;
			}

			// commit write operation.
			void RequestCommit(const uint64_t &timestamp, volatile bool *is_ready){
				spinlock_.Lock();
				// thomas write rule: the write is stale. remove it from buffer and return.
				if (timestamp < write_ts_){
					// get the write entry that can be committed.
					RequestEntry *entry = DebufferWriteRequest(timestamp);
					// blocked read requests can be unblocked.
					UpdateBuffer();
					delete entry;
					entry = NULL;
				}
				// if there is still some reads, then buffer the commit.
				else if (timestamp > min_read_ts_) {
					BufferCommitRequest(timestamp, is_ready);
				}
				// otherwise, we can write, and update the write ts.
				else {
					// get the matching write.
					RequestEntry *entry = DebufferWriteRequest(timestamp);
					// install the value.
					memcpy(data_ptr_, *entry->data_, data_size_);
					// update the write ts.
					if (write_ts_ < timestamp) {
						write_ts_ = timestamp;
					}
					UpdateBuffer();
					delete entry;
					entry = NULL;
				}
				spinlock_.Unlock();
			}

			void RequestAbort(const uint64_t &timestamp) {
				spinlock_.Lock();
				RequestEntry *entry = DebufferWriteRequest(timestamp);
				UpdateBuffer();
				spinlock_.Unlock();
				delete entry;
				entry = NULL;
			}

		private:
			void BufferReadRequest(const uint64_t &timestamp, char **data, volatile bool *is_ready){
				RequestEntry *entry = new RequestEntry();
				entry->timestamp_ = timestamp;
				entry->is_ready_ = is_ready;
				entry->data_ = data;
				/*entry->next_ = read_requests_head_;
				// become the head of the read request queue.
				// TODO: optimization: maintain a priority queue.
				read_requests_head_ = entry;*/
				RequestEntry* tmp = read_requests_head_;
				RequestEntry* pre = NULL;
				while(tmp != NULL && tmp->timestamp_ > timestamp){
					pre = tmp;
					tmp = tmp->next_;
				}
				entry->next_ = tmp;
				if(pre != NULL){
					pre->next_ = entry;
				}
				else{
					read_requests_head_ = entry;
				}
				// update minimum ts in the read queue.
				if (timestamp < min_read_ts_) {
					min_read_ts_ = timestamp;
				}
			}

			void BufferWriteRequest(const uint64_t &timestamp, char **data){
				RequestEntry *entry = new RequestEntry();
				entry->timestamp_ = timestamp;
				entry->data_ = data;
				/*entry->next_ = write_requests_head_;
				// become the head of the write request queue.
				// TODO: optimization: maintain a priority queue.
				write_requests_head_ = entry;*/
				RequestEntry* tmp = write_requests_head_;
				RequestEntry* pre = NULL;
				while(tmp != NULL && tmp->timestamp_ > timestamp){
					pre = tmp;
					tmp = tmp->next_;
				}
				entry->next_ = tmp;
				if(pre != NULL){
					pre->next_ = entry;
				}
				else{
					write_requests_head_ = entry;
				}
				// update minimum ts in the write queue.
				if (timestamp < min_write_ts_) {
					min_write_ts_ = timestamp;
				}
			}

			void BufferCommitRequest(const uint64_t &timestamp, volatile bool *is_ready){
				RequestEntry *entry = new RequestEntry();
				entry->timestamp_ = timestamp;
				entry->is_ready_ = is_ready;
				/*entry->next_ = commit_requests_head_;
				commit_requests_head_ = entry;*/
				RequestEntry* tmp = commit_requests_head_;
				RequestEntry* pre = NULL;
				while(tmp != NULL && tmp->timestamp_ > timestamp){
					pre = tmp;
					tmp = tmp->next_;
				}
				entry->next_ = tmp;
				if(pre != NULL){
					pre->next_ = entry;
				}
				else{
					commit_requests_head_ = entry;
				}
			}

			RequestEntry *DebufferReadRequest() {
				RequestEntry *ret_entry = NULL;
				RequestEntry *tmp_entry = read_requests_head_;
				RequestEntry *prev_entry = NULL;
				// get a list of read requests that can proceed now.
				while(tmp_entry != NULL && tmp_entry->timestamp_ > min_write_ts_){
					prev_entry = tmp_entry;
					tmp_entry = tmp_entry->next_;
				}
				ret_entry = tmp_entry;
				if(prev_entry != NULL){
					prev_entry->next_ = NULL;
				}
				else{
					read_requests_head_ = NULL;
				}
				/*while (tmp_entry != NULL) {
					if (tmp_entry->timestamp_ <= min_write_ts_) {
						if (prev_entry != NULL) {
							prev_entry->next_ = tmp_entry->next_;
						}
						else {
							read_requests_head_ = tmp_entry->next_;
						}
						tmp_entry->next_ = ret_entry;
						ret_entry = tmp_entry;
						if (prev_entry != NULL) {
							tmp_entry = prev_entry->next_;
						}
						else {
							tmp_entry = read_requests_head_;
						}
					}
					else {
						prev_entry = tmp_entry;
						tmp_entry = tmp_entry->next_;
					}
				}*/
				return ret_entry;
			}

			// we can always get exactly one matching request.
			RequestEntry *DebufferWriteRequest(const uint64_t &timestamp) {
				assert(write_requests_head_ != NULL);

				RequestEntry *ret_entry = write_requests_head_;
				RequestEntry *prev_entry = NULL;
				while (ret_entry != NULL && ret_entry->timestamp_ != timestamp) {
					prev_entry = ret_entry;
					ret_entry = ret_entry->next_;
				}
				// we must find exactly one matcing request.
				assert(ret_entry != NULL);

				if (prev_entry != NULL) {
					prev_entry->next_ = ret_entry->next_;
				}
				else {
					write_requests_head_ = ret_entry->next_;
				}
				ret_entry->next_ = NULL;
				return ret_entry;
			}

			// we can get a list of matching request
			RequestEntry *DebufferCommitRequest(){
				RequestEntry *ret_entry = NULL;
				RequestEntry *tmp_entry = commit_requests_head_;
				RequestEntry *prev_entry = NULL;
				// get a list of read requests that can proceed now.
				while(tmp_entry != NULL && tmp_entry->timestamp_ > min_read_ts_){
					prev_entry = tmp_entry;
					tmp_entry = tmp_entry->next_;
				}
				ret_entry = tmp_entry;
				if(prev_entry != NULL){
					prev_entry->next_ = NULL;
				}
				else{
					commit_requests_head_ = NULL;
				}
				/*while (tmp_entry != NULL) {
					if (tmp_entry->timestamp_ <= min_read_ts_) {
						if (prev_entry != NULL) {
							prev_entry->next_ = tmp_entry->next_;
						}
						else {
							commit_requests_head_ = tmp_entry->next_;
						}
						tmp_entry->next_ = ret_entry;
						ret_entry = tmp_entry;
						if (prev_entry != NULL) {
							tmp_entry = prev_entry->next_;
						}
						else {
							tmp_entry = commit_requests_head_;
						}
					}
					else {
						prev_entry = tmp_entry;
						tmp_entry = tmp_entry->next_;
					}
				}*/
				return ret_entry;
			}

			// this function is always called after write has been installed or aborted.
			void UpdateBuffer() {
				while (true) {
					// the committed write can be the minimum one. then we need to update min_write_ts.
					uint64_t new_wts = GetMinWriteTimestamp();
					assert(new_wts >= min_write_ts_);
					if (new_wts == min_write_ts_){
						return;
					}
					min_write_ts_ = new_wts;
					// since the min_write_ts has been updated, then probably a list of read request can be issued.
					RequestEntry *read_list = DebufferReadRequest();
					if (read_list == NULL) {
						return;
					}
					// allow these reads to be proceeded.
					RequestEntry *read_entry = read_list;
					while (read_entry != NULL) {
						// copy data here.
						// data has already been allocated.
						memcpy(*(read_entry->data_), data_ptr_, data_size_);
						// directly read.
						if (read_ts_ < read_entry->timestamp_) {
							read_ts_ = read_entry->timestamp_;
						};
						// inform the blocked threads.
						*(read_entry->is_ready_) = true;
						// destroy these read requests.
						RequestEntry *tmp_entry = read_entry;
						read_entry = read_entry->next_;
						delete tmp_entry;
						tmp_entry = NULL;
					}
					// read request queue has been updated. then we need to update min_read_ts.
					uint64_t new_rts = GetMinReadTimestamp();
					assert(new_rts >= min_read_ts_);
					if (new_rts == min_read_ts_){
						return;
					}
					min_read_ts_ = new_rts;
					RequestEntry *commit_list = DebufferCommitRequest();
					if (commit_list == NULL) {
						return;
					}
					RequestEntry *win_commit = NULL;
					uint64_t win_ts = 0;
					RequestEntry *commit_entry = commit_list;
					while (commit_entry != NULL) {
						RequestEntry *tmp_write_entry = DebufferWriteRequest(commit_entry->timestamp_);
						assert(tmp_write_entry != NULL);
						if (commit_entry->timestamp_ > win_ts){
							delete win_commit;
							win_commit = commit_entry;
							win_ts = commit_entry->timestamp_;
						}
						else{
							delete commit_entry;
							commit_entry = NULL;
						}
						*(commit_entry->is_ready_) = true;
						RequestEntry *tmp_commit_entry = commit_entry;
						commit_entry = commit_entry->next_;
						delete tmp_commit_entry;
						tmp_commit_entry = NULL;
					}
					assert(win_commit != NULL);
					// perform write.
					// install the value.
					memcpy(data_ptr_, *win_commit->data_, data_size_);
					// update the write ts.
					if (write_ts_ < win_commit->timestamp_) {
						write_ts_ = win_commit->timestamp_;
					}
					delete win_commit;
					win_commit = NULL;
				}
			}

			uint64_t GetMinReadTimestamp() const {
				RequestEntry *tmp_entry = read_requests_head_;
				return GetMinTimestamp(tmp_entry);
			}

			uint64_t GetMinWriteTimestamp() const {
				RequestEntry *tmp_entry = write_requests_head_;
				return GetMinTimestamp(tmp_entry);
			}

			uint64_t GetMinTimestamp(RequestEntry *tmp_entry) const{
				uint64_t new_min_ts = INT64_MAX;
				// the request list is sorted from big to small
				// so that we should retrieve the whole list
				// we want to keep the reverse sorted order because it is beneficial for DebufferRead and DebufferWrite(these functions are called more frequently)
				while (tmp_entry != NULL) {
					if (tmp_entry->timestamp_ < new_min_ts) {
						new_min_ts = tmp_entry->timestamp_;
					}
					tmp_entry = tmp_entry->next_;
				}
				return new_min_ts;
			}

		private:
			char *data_ptr_;
			size_t data_size_;
			// last read that has been issued.
			uint64_t read_ts_;
			// last write that has been issued.
			uint64_t write_ts_;
			// minimum read that is waiting in the queue.
			uint64_t min_read_ts_;
			// minimum write that is waiting in the queue.
			uint64_t min_write_ts_;
			// minimum commit that is waiting in the queue.
			//uint64_t min_commit_ts_;
			// read request queue.
			RequestEntry *read_requests_head_;
			// write request queue.
			RequestEntry *write_requests_head_;
			// commit request queue.
			RequestEntry *commit_requests_head_;
			SpinLock spinlock_;
		};
	}
}

#endif
