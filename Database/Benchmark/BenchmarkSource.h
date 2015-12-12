#pragma once
#ifndef __CAVALIA_BENCHMARK_BENCHMARK_SOURCE_H__
#define __CAVALIA_BENCHMARK_BENCHMARK_SOURCE_H__

#include <iostream>
#include <fstream>
#include <TimeMeasurer.h>
#include <boost/filesystem.hpp>
#include "BenchmarkScaleParams.h"
#include "../Redirector/IORedirector.h"

namespace Cavalia{
	namespace Benchmark{
		using namespace Cavalia::Database;

		enum SourceType : size_t { RANDOM_SOURCE, PARTITION_SOURCE, SELECT_SOURCE };
		class BenchmarkSource{
		public:
			BenchmarkSource(const std::string &prefix, IORedirector *redirector, BenchmarkScaleParams *const scale_params, const size_t &num_transactions, const size_t dist_ratio) : redirector_ptr_(redirector), num_transactions_(num_transactions), dist_ratio_(dist_ratio), source_type_(RANDOM_SOURCE), partition_count_(0), partition_id_(0){
				log_filename_ = prefix + "_" + scale_params->ToString() + "_" + std::to_string(num_transactions) + "_" + std::to_string(dist_ratio) + "_" + std::to_string(source_type_);
				is_exists_ = boost::filesystem::exists(log_filename_);
			}

			BenchmarkSource(const std::string &prefix, IORedirector *redirector, BenchmarkScaleParams *const scale_params, const size_t &num_transactions, const size_t &dist_ratio, const size_t &partition_count) : redirector_ptr_(redirector), num_transactions_(num_transactions), dist_ratio_(dist_ratio), source_type_(PARTITION_SOURCE), partition_count_(partition_count), partition_id_(0){
				log_filename_ = prefix + "_" + scale_params->ToString() + "_" + std::to_string(num_transactions) + "_" + std::to_string(dist_ratio) + "_" + std::to_string(source_type_) + "_" + std::to_string(partition_count);
				is_exists_ = boost::filesystem::exists(log_filename_);
			}

			BenchmarkSource(const std::string &prefix, IORedirector *redirector, BenchmarkScaleParams *const scale_params, const size_t &num_transactions, const size_t &dist_ratio, const size_t &partition_count, const size_t &partition_id) : redirector_ptr_(redirector), num_transactions_(num_transactions), dist_ratio_(dist_ratio), source_type_(SELECT_SOURCE), partition_count_(partition_count), partition_id_(partition_id){
				log_filename_ = prefix + "_" + scale_params->ToString() + "_" + std::to_string(num_transactions) + "_" + std::to_string(dist_ratio) + "_" + std::to_string(source_type_) + "_" + std::to_string(partition_count) + "_" + std::to_string(partition_id);
				is_exists_ = boost::filesystem::exists(log_filename_);
			}

			virtual ~BenchmarkSource(){}

			void Start(){
				TimeMeasurer timer;
				timer.StartTimer();
				if (is_exists_ == true) {
					// load from file.
					ReloadFromDisk();
				}
				else {
					// generate params and dump to file.
					output_file_.open(log_filename_, std::ofstream::binary);
					StartGeneration();
					output_file_.close();
				}
				timer.EndTimer();
				std::cout << "source elapsed time=" << timer.GetElapsedMilliSeconds() << "ms" << std::endl;
			}

		protected:
			void DumpToDisk(ParamBatch *tuples) {
				for (size_t i = 0; i < tuples->size(); ++i) {
					TxnParam *tuple = tuples->get(i);
					CharArray param_chars;
					tuple->Serialize(param_chars);
					// write stored procedure type.
					size_t tuple_type = tuple->type_;
					output_file_.write((char*)(&tuple_type), sizeof(tuple_type));
					// write parameter size.
					output_file_.write((char*)(&param_chars.size_), sizeof(param_chars.size_));
					// write parameters.
					output_file_.write(param_chars.char_ptr_, param_chars.size_);
					output_file_.flush();
					param_chars.Release();
				}
			}

		private:
			virtual void ReloadFromDisk() {
				std::ifstream log_reloader(log_filename_, std::ifstream::binary);
				assert(log_reloader.good() == true);
				log_reloader.seekg(0, std::ios::end);
				size_t file_size = static_cast<size_t>(log_reloader.tellg());
				log_reloader.seekg(0, std::ios::beg);
				size_t file_pos = 0;
				CharArray entry;
				entry.Allocate(10240);
				ParamBatch *tuples = new ParamBatch(gParamBatchSize);
				while (file_pos < file_size) {
					size_t param_type;
					log_reloader.read(reinterpret_cast<char*>(&param_type), sizeof(param_type));
					file_pos += sizeof(param_type);
					log_reloader.read(reinterpret_cast<char*>(&entry.size_), sizeof(entry.size_));
					file_pos += sizeof(entry.size_);
					if (file_size - file_pos >= entry.size_) {
						log_reloader.read(entry.char_ptr_, entry.size_);
						TxnParam* event_tuple = DeserializeParam(param_type, entry);
						if (event_tuple != NULL) {
							tuples->push_back(event_tuple);
							if (tuples->size() == gParamBatchSize) {
								redirector_ptr_->PushParameterBatch(tuples);
								tuples = new ParamBatch(gParamBatchSize);
							}
						}
						file_pos += entry.size_;
					}
					else {
						break;
					}
				}
				if (tuples->size() != 0) {
					redirector_ptr_->PushParameterBatch(tuples);
				}
				else {
					delete tuples;
					tuples = NULL;
				}
				entry.Release();
				log_reloader.close();
			
			}

			virtual TxnParam* DeserializeParam(const size_t &param_type, const CharArray&) = 0;
			virtual void StartGeneration() = 0;

		private:
			BenchmarkSource(const BenchmarkSource &);
			BenchmarkSource& operator=(const BenchmarkSource &);

		protected:
			IORedirector *const redirector_ptr_;
			const size_t num_transactions_;
			const size_t dist_ratio_;
			const SourceType source_type_;
			const size_t partition_count_;
			const size_t partition_id_;
			bool is_exists_;
			std::string log_filename_;
			std::ofstream output_file_;
		};
	}
}

#endif
