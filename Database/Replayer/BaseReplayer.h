#pragma once
#ifndef __CAVALIA_DATABASE_BASE_REPLAYER_H__
#define __CAVALIA_DATABASE_BASE_REPLAYER_H__

#include <unordered_map>
#include <thread>
#include "NullLogger.h"
#include "StoredProcedure.h"
#include "StoredProcedure.h"

namespace Cavalia{
	namespace Database{
		class BaseReplayer{
		public:
			BaseReplayer(const std::string &filename, BaseStorageManager *const storage_manager, const size_t &thread_count) : filename_(filename), storage_manager_(storage_manager), thread_count_(thread_count){}
			virtual ~BaseReplayer(){
				for (auto &entry : log_buffer_){
					delete entry.second;
					entry.second = NULL;
				}
			}

			virtual void Start(){
				Register();
				TimeMeasurer timer;
				timer.StartTimer();
				ReloadAll();
				timer.EndTimer();
				std::cout << "reload log elapsed time=" << timer.GetElapsedMilliSeconds() << "ms" << std::endl;

				timer.StartTimer();
				ProcessLog();
				timer.EndTimer();
				std::cout << "process log elapsed time=" << timer.GetElapsedMilliSeconds() << "ms" << std::endl;
			}

		private:
			virtual EventTuple* DeserializeParam(const size_t &param_type, const CharArray&) = 0;
			virtual void ReloadAll(){
				std::ifstream log_reloader(filename_, std::ifstream::binary);
				assert(log_reloader.good() == true);
				log_reloader.seekg(0, std::ios::end);
				size_t file_size = static_cast<size_t>(log_reloader.tellg());
				log_reloader.seekg(0, std::ios::beg);
				size_t file_pos = 0;
				CharArray entry;
				entry.Allocate(1024);
				while (file_pos < file_size){
					size_t param_type;
					log_reloader.read(reinterpret_cast<char*>(&param_type), sizeof(param_type));
					file_pos += sizeof(param_type);
					log_reloader.read(reinterpret_cast<char*>(&entry.size_), sizeof(entry.size_));
					file_pos += sizeof(entry.size_);
					if (file_size - file_pos >= entry.size_){
						log_reloader.read(entry.char_ptr_, entry.size_);
						EventTuple* event_tuple = DeserializeParam(param_type, entry);
						if (event_tuple != NULL){
							log_buffer_.push_back(std::make_pair(param_type, event_tuple));
						}
						file_pos += entry.size_;
					}
					else{
						break;
					}
				}
				entry.Release();
				log_reloader.close();
			}

			virtual void Register() = 0;
			virtual void ProcessLog() = 0;

		private:
			BaseReplayer(const BaseReplayer &);
			BaseReplayer& operator=(const BaseReplayer &);

		protected:
			std::string filename_;
			BaseStorageManager *const storage_manager_;
			NullLogger logger_;
			size_t thread_count_;
			InputBatch log_buffer_;
		};
	}
}

#endif
