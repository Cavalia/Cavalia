#pragma once
#ifndef __CAVALIA_TPCC_BENCHMARK_TPCC_CONFIGURE_H__
#define __CAVALIA_TPCC_BENCHMARK_TPCC_CONFIGURE_H__

#include <string>
#include <cstdint>
#include <vector>
#include <fstream>

namespace Cavalia{
	namespace Benchmark{
		namespace Tpcc{
			class ConfigFileParser{
			public:
				// config file format: 
				void LogConfigFile(const int &server_id, const bool &is_horizontal){
					// load server ip.
					std::ifstream servers_infile;
					servers_infile.open("conf/servers.txt");
					assert(servers_infile.good() == true);
					std::string addr;
					while (std::getline(servers_infile, addr)){
						peer_ips_.push_back(addr);
					}
					servers_infile.close();
					if (server_id != -1){
						self_ip_ = peer_ips_.at(server_id);
					}

					// load data distribution
					if (is_horizontal == true){
						std::ifstream tables_infile("conf/htables.txt");
						assert(tables_infile.good() == true);
						std::string partition;
						while (std::getline(tables_infile, partition)){
							size_t pos = partition.find("-");
							std::string lh = partition.substr(0, pos);
							std::string rh = partition.substr(pos + 1);
							partitions_.push_back(std::make_pair(atoi(lh.c_str()), atoi(rh.c_str())));
						}
						tables_infile.close();
					}
					else{
						std::ifstream tables_infile("conf/vtables.txt");
						assert(tables_infile.good() == true);
						tables_infile.close();
					}
				}

				std::string self_ip_;
				std::vector<std::string> peer_ips_;
				std::vector<std::pair<size_t, size_t>> partitions_;
			};
		}
	}
}

#endif
