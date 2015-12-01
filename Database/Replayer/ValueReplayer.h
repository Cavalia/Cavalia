#pragma once
#ifndef __CAVALIA_DATABASE_VALUE_REPLAYER_H__
#define __CAVALIA_DATABASE_VALUE_REPLAYER_H__

#include "../Storage/BaseStorageManager.h"
#include "BaseReplayer.h"

namespace Cavalia{
	namespace Database{
		struct ValueStruct{
			uint8_t type_;
			size_t table_id_;
			size_t data_size_;
			char *data_ptr_;
		};

		class ValueReplayer : public BaseReplayer{
		public:
			ValueReplayer(const std::string &filename, BaseStorageManager *const storage_manager, const size_t &thread_count) : BaseReplayer(filename, storage_manager, thread_count, true){}
			virtual ~ValueReplayer(){}

			virtual void Start(){
				boost::thread_group reloaders;
				for (size_t i = 0; i < thread_count_; ++i){
					reloaders.create_thread(boost::bind(&ValueReplayer::ReloadLog, this, i));
				}
				reloaders.join_all();
			}

			void ReloadLog(const size_t &thread_id){
				FILE *infile_ptr = infiles_[thread_id];
				fseek(infile_ptr, 0L, SEEK_END);
				size_t file_size = ftell(infile_ptr);
				rewind(infile_ptr);
				size_t file_pos = 0;
				int result = 0;
				while (file_pos < file_size){
					size_t txn_size;
					result = fread(&txn_size, sizeof(txn_size), 1, infile_ptr);
					assert(result == 1);
					size_t txn_pos = 0;
					while (txn_pos < txn_size){
						ValueStruct *vs_ptr = new ValueStruct();
						result = fread(&vs_ptr->type_, sizeof(vs_ptr->type_), 1, infile_ptr);
						assert(result == 1);
						txn_pos += sizeof(vs_ptr->type_);
						result = fread(&vs_ptr->table_id_, sizeof(vs_ptr->table_id_), 1, infile_ptr);
						assert(result == 1);
						txn_pos += sizeof(vs_ptr->table_id_);
						result = fread(&vs_ptr->data_size_, sizeof(vs_ptr->data_size_), 1, infile_ptr);
						assert(result == 1);
						txn_pos += sizeof(vs_ptr->data_size_);
						vs_ptr->data_ptr_ = new char[vs_ptr->data_size_];
						result = fread(vs_ptr->data_ptr_, 1, vs_ptr->data_size_, infile_ptr);
						assert(result == vs_ptr->data_size_);
						txn_pos += vs_ptr->data_size_;
						value_log_.push_back(vs_ptr);
					}
					assert(txn_pos == txn_size);
					file_pos += sizeof(txn_size)+txn_size;
				}
				assert(file_pos == file_size);
			}

			void ProcessLog(const size_t &thread_id){

			}

		private:
			ValueReplayer(const ValueReplayer &);
			ValueReplayer& operator=(const ValueReplayer &);

		private:
			std::vector<ValueStruct*> value_log_;
		};
	}
}

#endif
