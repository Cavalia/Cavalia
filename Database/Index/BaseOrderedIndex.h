#pragma once
#ifndef __CAVALIA_DATABASE_BASE_ORDERED_INDEX_H__
#define __CAVALIA_DATABASE_BASE_ORDERED_INDEX_H__

#include <string>
#include "../Storage/TableRecords.h"

namespace Cavalia{
	namespace Database{
		class BaseOrderedIndex{
		public:
			BaseOrderedIndex(){}
			virtual ~BaseOrderedIndex(){}

			virtual void InsertRecord(const std::string&, TableRecord *) = 0;
			virtual void DeleteRecord(const std::string&) = 0;
			virtual TableRecord* SearchRecord(const std::string&) = 0;
			virtual void SearchRecords(const std::string&, TableRecords*) = 0;
			virtual void SearchUpperRecords(const std::string&, TableRecords*) = 0;
			virtual void SearchLowerRecords(const std::string&, TableRecords*) = 0;
			virtual void SearchRangeRecords(const std::string&, const std::string&, TableRecords*) = 0;

		private:
			BaseOrderedIndex(const BaseOrderedIndex &);
			BaseOrderedIndex& operator=(const BaseOrderedIndex &);
		};
	}
}

#endif
