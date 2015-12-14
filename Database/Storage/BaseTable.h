#pragma once
#ifndef __CAVALIA_DATABASE_BASE_TABLE_H__
#define __CAVALIA_DATABASE_BASE_TABLE_H__

#include <AllocatorHelper.h>
#include <vector>
#include <string>
#include <fstream>
#include "TableRecord.h"
#include "TableRecords.h"

namespace Cavalia{
	namespace Database{
		class BaseTable {
		public:
			BaseTable(const RecordSchema * const schema_ptr) : schema_ptr_(schema_ptr), secondary_count_(schema_ptr->GetSecondaryCount()){}
			virtual ~BaseTable(){}

			// get the length of each record.
			const size_t& GetRecordSize() const {
				return schema_ptr_->GetSchemaSize();
			}

			// get the number of records in this table.
			virtual const size_t GetTableSize() const = 0;

			virtual bool InsertRecord(TableRecord *record) = 0;
			virtual bool InsertRecord(const std::string &key, TableRecord *record) = 0;
			virtual void DeleteRecord(TableRecord *record) = 0;
			virtual void DeleteRecord(const std::string &key, TableRecord *record) = 0;

			///////////////////NEW API//////////////////
			virtual void SelectKeyRecord(const std::string &key, TableRecord *&record) const = 0;
			virtual void SelectKeyRecord(const size_t &part_id, const std::string &key, TableRecord *&record) const = 0;
			virtual void SelectRecord(const size_t &idx_id, const std::string &key, TableRecord *&record) const = 0;
			virtual void SelectRecord(const size_t &part_id, const size_t &idx_id, const std::string &key, TableRecord *&record) const = 0;
			virtual void SelectRecords(const size_t &idx_id, const std::string &key, TableRecords *records) const = 0;
			virtual void SelectRecords(const size_t &part_id, const size_t &idx_id, const std::string &key, TableRecords *records) const = 0;
			virtual void SelectUpperRecords(const size_t &idx_id, const std::string &key, TableRecords *records) const = 0;
			virtual void SelectUpperRecords(const size_t part_id, const size_t &idx_id, const std::string &key, TableRecords *records) const = 0;
			virtual void SelectLowerRecords(const size_t &idx_id, const std::string &key, TableRecords *records) const = 0;
			virtual void SelectLowerRecords(const size_t part_id, const size_t &idx_id, const std::string &key, TableRecords *records) const = 0;
			virtual void SelectRangeRecords(const size_t &idx_id, const std::string &lower_key, std::string &upper_key, TableRecords *records) const = 0;
			virtual void SelectRangeRecords(const size_t part_id, const size_t &idx_id, const std::string &lower_key, const std::string &upper_key, TableRecords *records) const = 0;
			
			virtual void SaveCheckpoint(std::ofstream &out_stream) = 0;
			virtual void ReloadCheckpoint(std::ifstream &in_stream) = 0;

		private:
			BaseTable(const BaseTable&);
			BaseTable & operator=(const BaseTable&);

		protected:
			const RecordSchema *const schema_ptr_;
			const size_t secondary_count_;
		};
	}
}

#endif