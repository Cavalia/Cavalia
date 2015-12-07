#pragma once
#ifndef __CAVALIA_BENCHMARK_BENCHMARK_CONFIG_PARSER_H__
#define __CAVALIA_BENCHMARK_BENCHMARK_CONFIG_PARSER_H__

#include <string>
#include <cassert>
#include <cstdint>
#include <vector>
#include <fstream>

struct ServerInfo{
	std::string addr_;
	size_t min_part_;
	size_t max_part_;
};

class BenchmarkConfigParser{
public:
	void LogConfigFile(const bool &is_horizontal){
		// load server ip.
		std::ifstream servers_infile;
		servers_infile.open("conf/servers.txt");
		assert(servers_infile.good() == true);
		std::string line;
		while (std::getline(servers_infile, line)){
			ServerInfo info;
			size_t pos = line.find(",");
			std::string addr = line.substr(0, pos);
			info.addr_ = addr;
			std::string part = line.substr(pos + 1);
			pos = part.find("-");
			std::string lh = part.substr(0, pos);
			std::string rh = part.substr(pos + 1);
			info.min_part_ = atoi(lh.c_str());
			info.max_part_ = atoi(rh.c_str());
			servers_.push_back(info);
		}
		servers_infile.close();
	}

	size_t GetPartitionCount() const {
		return servers_.at(servers_.size() - 1).max_part_;
	}

	size_t GetServerCount() const {
		return servers_.size();
	}

	std::vector<ServerInfo> GetServerInfo(){
		return servers_;
	}

private:
	std::vector<ServerInfo> servers_;

};

#endif
