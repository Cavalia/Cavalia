#include "MicroPopulator.h"


namespace Cavalia{
	namespace Benchmark{
		namespace Micro{
			void MicroPopulator::StartPopulate(){
				std::cout << "num accounts=" << num_accounts_ << std::endl;
				for (int accid = 1; accid <= num_accounts_; ++accid){
					MicroRecord* record_ptr = GenerateMicroRecord(accid);
					InsertMicroRecord(record_ptr);
					delete record_ptr;
					record_ptr = NULL;
				}
			}

			void MicroPopulator::InsertMicroRecord(const MicroRecord* record_ptr){
				char *data = new char[MicroSchema::GenerateMicroSchema()->GetSchemaSize()];
				SchemaRecord *record = new SchemaRecord(MicroSchema::GenerateMicroSchema(), data);
				record->SetColumn(0, reinterpret_cast<const char*>(&record_ptr->key_));
				record->SetColumn(1, reinterpret_cast<const char*>(record_ptr->value_));
				storage_manager_->tables_[MICRO_TABLE_ID]->InsertRecord(new TableRecord(record));
			}

			MicroRecord* MicroPopulator::GenerateMicroRecord(const int& key) const{
				MicroRecord* record = new MicroRecord();
				record->key_ = static_cast<int64_t>(key);
				std::string value = MicroRandomGenerator::GenerateAccountName(key);
				assert(value.length() == VALUE_LEN);
				memcpy(record->value_, value.c_str(), value.size());
				return record;
			}
		}
	}
}