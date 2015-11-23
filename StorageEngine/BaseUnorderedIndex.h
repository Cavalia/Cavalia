#pragma once
#ifndef __CAVALIA_STORAGE_ENGINE_BASE_UNORDERED_INDEX_H__
#define __CAVALIA_STORAGE_ENGINE_BASE_UNORDERED_INDEX_H__

#include <string>
#include <fstream>
#include "TableRecord.h"

namespace Cavalia{
	namespace StorageEngine{
		class BaseUnorderedIndex{
		public:
			BaseUnorderedIndex(){}
			virtual ~BaseUnorderedIndex(){}

			virtual void InsertRecord(const std::string&, TableRecord *) = 0;
			virtual void DeleteRecord(const std::string&) = 0;
			virtual TableRecord* SearchRecord(const std::string&) = 0;
			virtual size_t GetSize() const = 0;
			virtual void SaveCheckpoint(std::ofstream &, const size_t &) = 0;

		private:
			BaseUnorderedIndex(const BaseUnorderedIndex &);
			BaseUnorderedIndex& operator=(const BaseUnorderedIndex &);
		};
	}
}

#endif
