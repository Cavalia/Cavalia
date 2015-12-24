#pragma once
#ifndef __CAVALIA_DATABASE_SHARE_TABLE_H__
#define __CAVALIA_DATABASE_SHARE_TABLE_H__

#include "BaseTable.h"
#include "../Index/StdUnorderedIndex.h"
#include "../Index/StdUnorderedIndexMT.h"
#include "../Index/StdOrderedIndex.h"
#include "../Index/StdOrderedIndexMT.h"
#if defined(CUCKOO_INDEX)
#include "../Index/CuckooIndex.h"
#endif

namespace Cavalia{
	namespace Database{
		class ShareTable : public BaseTable {
		public:
			ShareTable(const RecordSchema * const schema_ptr, bool is_thread_safe) : BaseTable(schema_ptr){
				if (is_thread_safe == true){
#if defined(CUCKOO_INDEX)
					primary_index_ = new CuckooIndex();
#else
					primary_index_ = new StdUnorderedIndexMT();
#endif
					secondary_indexes_ = new BaseOrderedIndex*[secondary_count_];
					for (size_t i = 0; i < secondary_count_; ++i){
						secondary_indexes_[i] = new StdOrderedIndexMT();
					}
				}
				else{
					primary_index_ = new StdUnorderedIndex();
					secondary_indexes_ = new BaseOrderedIndex*[secondary_count_];
					for (size_t i = 0; i < secondary_count_; ++i){
						secondary_indexes_[i] = new StdOrderedIndex();
					}
				}
			}

			virtual ~ShareTable(){
				// records in the table is released by primary index.
				delete primary_index_;
				primary_index_ = NULL;
				for (size_t i = 0; i < secondary_count_; ++i){
					delete secondary_indexes_[i];
					secondary_indexes_[i] = NULL;
				}
				delete[] secondary_indexes_;
				secondary_indexes_ = NULL;
			}

			// get the number of records in this table.
			virtual const size_t GetTableSize() const {
				return primary_index_->GetSize();
			}

			///////////////////INSERT//////////////////
			virtual bool InsertRecord(TableRecord *record){
				SchemaRecord *record_ptr = record->record_;
				if (primary_index_->InsertRecord(record_ptr->GetPrimaryKey(), record) == true){
					// build secondary index here
					for (size_t i = 0; i < secondary_count_; ++i){
						secondary_indexes_[i]->InsertRecord(record_ptr->GetSecondaryKey(i), record);
					}
					return true;
				}
				else{
					return false;
				}
			}

			virtual bool InsertRecord(const std::string &primary_key, TableRecord *record){
				if (primary_index_->InsertRecord(primary_key, record) == true){
					SchemaRecord *record_ptr = record->record_;
					// build secondary index here
					for (size_t i = 0; i < secondary_count_; ++i){
						secondary_indexes_[i]->InsertRecord(record_ptr->GetSecondaryKey(i), record);
					}
					return true;
				}
				else{
					return false;
				}
			}
			
			/////////////////////DELETE//////////////////
			virtual void DeleteRecord(TableRecord *record){
				SchemaRecord *record_ptr = record->record_;
				primary_index_->DeleteRecord(record_ptr->GetPrimaryKey());
				// update secondary index here
				for (size_t i = 0; i < secondary_count_; ++i){
					secondary_indexes_[i]->DeleteRecord(record_ptr->GetSecondaryKey(i));
				}
				// ========================IMPORTANT========================
				// TODO: deletion should rely on a garbage collector. potential memory leak!
			}

			virtual void DeleteRecord(const std::string &primary_key, TableRecord *record) {
				primary_index_->DeleteRecord(primary_key);
				SchemaRecord *record_ptr = record->record_;
				for (size_t i = 0; i < secondary_count_; ++i) {
					secondary_indexes_[i]->DeleteRecord(record_ptr->GetSecondaryKey(i));
				}
			}

			///////////////////NEW API//////////////////
			virtual void SelectKeyRecord(const std::string &primary_key, TableRecord *&record) const {
				record = primary_index_->SearchRecord(primary_key);
			}

			virtual void SelectKeyRecord(const size_t &part_id, const std::string &primary_key, TableRecord *&record) const {
				assert(false);
			}

			virtual void SelectRecord(const size_t &idx_id, const std::string &key, TableRecord *&record) const {
				record = secondary_indexes_[idx_id]->SearchRecord(key);
			}

			virtual void SelectRecord(const size_t &part_id, const size_t &idx_id, const std::string &key, TableRecord *&record) const {
				assert(false);
			}

			virtual void SelectRecords(const size_t &idx_id, const std::string &key, TableRecords *records) const {
				secondary_indexes_[idx_id]->SearchRecords(key, records);
			}

			virtual void SelectRecords(const size_t &part_id, const size_t &idx_id, const std::string &key, TableRecords *records) const {
				assert(false);
			}

			virtual void SelectUpperRecords(const size_t &idx_id, const std::string &key, TableRecords *records) const {
				secondary_indexes_[idx_id]->SearchUpperRecords(key, records);
			}

			virtual void SelectUpperRecords(const size_t part_id, const size_t &idx_id, const std::string &key, TableRecords *records) const {
				assert(false);
			}

			virtual void SelectLowerRecords(const size_t &idx_id, const std::string &key, TableRecords *records) const {
				secondary_indexes_[idx_id]->SearchLowerRecords(key, records);
			}

			virtual void SelectLowerRecords(const size_t part_id, const size_t &idx_id, const std::string &key, TableRecords *records) const {
				assert(false);
			}

			virtual void SelectRangeRecords(const size_t &idx_id, const std::string &lower_key, std::string &upper_key, TableRecords *records) const {
				secondary_indexes_[idx_id]->SearchRangeRecords(lower_key, upper_key, records);
			}

			virtual void SelectRangeRecords(const size_t part_id, const size_t &idx_id, const std::string &lower_key, const std::string &upper_key, TableRecords *records) const {
				assert(false);
			}


			virtual void SaveCheckpoint(std::ofstream &out_stream){
				size_t record_size = schema_ptr_->GetSchemaSize();
				primary_index_->SaveCheckpoint(out_stream, record_size);
			}

			virtual void ReloadCheckpoint(std::ifstream &in_stream){
				size_t record_size = schema_ptr_->GetSchemaSize();
				in_stream.seekg(0, std::ios::end);
				size_t file_size = static_cast<size_t>(in_stream.tellg());
				in_stream.seekg(0, std::ios::beg);
				size_t file_pos = 0;
				while (file_pos < file_size){
					char *entry = new char[record_size];
					in_stream.read(entry, record_size);
					TableRecord *record_ptr = new TableRecord(new SchemaRecord(schema_ptr_, entry));
					//if (schema_ptr_->GetTableId() >= 2 && schema_ptr_->GetTableId() <= 5)
					//	record_ptr->content_.SetHot(true);
					InsertRecord(record_ptr);
					file_pos += record_size;
				}
			}

		private:
			ShareTable(const ShareTable&);
			ShareTable & operator=(const ShareTable&);

		protected:
			BaseUnorderedIndex *primary_index_;
			BaseOrderedIndex **secondary_indexes_;
		};
	}
}

#endif
