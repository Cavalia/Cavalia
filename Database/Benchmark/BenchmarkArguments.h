#pragma once
#ifndef __CAVALIA_BENCHMARK_FRAMEWORK_BENCHMARK_ARGUMENTS_H__
#define __CAVALIA_BENCHMARK_FRAMEWORK_BENCHMARK_ARGUMENTS_H__

#include <iostream>
#include <cassert>
#include "../Meta/MetaTypes.h"

enum AppType { 
	APP_POPULATE, 
	APP_REPLAY, 
	APP_CC_EXECUTE, 
	APP_HSTORE_EXECUTE, 
	APP_SITE_EXECUTE, 
	APP_ISLAND_EXECUTE, 
	APP_SERVER_EXECUTE, 
	kAppSize 
};

enum AppReplayType { 
	APP_SERIAL_COMMAND_REPLAY, 
	APP_VALUE_REPLAY, 
	kAppReplaySize 
};

static std::string dir_name = ".";
static int app_type = -1;
static double scale_factors[2] = { -1, -1 };
static int factor_count = 0;
static int dist_ratio = 1;
static int num_txn = -1;
static int num_core = -1; // number of cores utilized in a single numa node.
static int num_node = -1; // number of nodes utilized.
static int replay_type = 0; // default: command replay
static int instance_id = -1;

static void PrintUsage() {
	std::cout << "==========[USAGE]==========" << std::endl;
	std::cout << "\t-aINT: APP_TYPE " << std::endl;
	std::cout << "\t\t0: POPULATE" << std::endl;
	std::cout << "\t\t1: REPLAY" << std::endl;
	std::cout << "\t\t2: CC_EXECUTE" << std::endl;
	std::cout << "\t\t3: HSTORE_EXECUTE" << std::endl;
	std::cout << "\t\t4: SITE_EXECUTE" << std::endl;
	std::cout << "\t\t5: ISLAND_EXECUTE" << std::endl;
	std::cout << "\t\t6: SERVER_EXECUTE" << std::endl;
	std::cout << "\t-sfDOUBLE: SCALE_FACTOR" << std::endl;
	std::cout << "\t-tINT: TXN_COUNT" << std::endl;
	std::cout << "\t-dINT: DIST_TXN_RATIO" << std::endl;
	std::cout << "\t-zINT: BATCH_SIZE" << std::endl;
	std::cout << "\t-cINT: CORE_COUNT" << std::endl;
	std::cout << "\t-nINT: NODE_COUNT" << std::endl;
	std::cout << "\t-rINT: REPLAY_TYPE (0: COMMAND [DEFAULT])" << std::endl;
	std::cout << "\t-iINT: INSTANCE_ID" << std::endl;
	std::cout << "===========================" << std::endl;
	std::cout << "==========[EXAMPLES]==========" << std::endl;
	std::cout << "<POPULATE> Benchmark -a0 -sf10 -sf100" << std::endl;
	std::cout << "<REPLAY> Benchmark -a1 -c1 [-r2]" << std::endl;
	std::cout << "<CC_EXECUTE> Benchmark -a2 -sf10 -sf100 -t100000 -c32" << std::endl;
	std::cout << "<HSTORE_EXECUTE> Benchmark -a3 -sf10 -sf100 -t100000 -c4 -n5 -d20" << std::endl;
	std::cout << "<SITE_EXECUTE> Benchmark -a4 -sf10 -sf100 -t100000 -c4 -n5 -d20" << std::endl;
	std::cout << "<ISLAND_EXECUTE> Benchmark -a5 -sf10 -sf100 -t100000 -i0" << std::endl;
	std::cout << "<SERVER_EXECUTE> Benchmark -a6 -sf10 -sf100 -t100000 -i0" << std::endl;
	std::cout << "==============================" << std::endl;
}

static void ArgumentsChecker() {
	if (app_type < 0 || app_type > kAppSize) {
		std::cout << "APP_TYPE (-a) should be in [0-" << kAppSize << "]." << std::endl;
		exit(0);
	}
	if (replay_type < 0 || replay_type > kAppReplaySize) {
		std::cout << "REPLAY_TYPE (-r) should be in [0-" << kAppReplaySize << "]." << std::endl;
		exit(0);
	}
	else if (app_type == APP_POPULATE) {
		if (factor_count == 0) {
			std::cout << "SCALE_FACTOR (-sf) should be set." << std::endl;
			exit(0);
		}
	}
	else if (app_type == APP_REPLAY) {
		if (num_core == -1) {
			std::cout << "CORE_COUNT (-c) should be set." << std::endl;
			exit(0);
		}
	}
	else if (app_type == APP_CC_EXECUTE || app_type == APP_SITE_EXECUTE || app_type == APP_HSTORE_EXECUTE) {
		if (factor_count == 0) {
			std::cout << "SCALE_FACTOR (-sf) should be set." << std::endl;
			exit(0);
		}
		if (num_core == -1) {
			std::cout << "CORE_COUNT (-c) should be set." << std::endl;
			exit(0);
		}
		if (num_txn == -1) {
			std::cout << "TXN_COUNT (-t) should be set." << std::endl;
			exit(0);
		}
		if (app_type == APP_SITE_EXECUTE || app_type == APP_HSTORE_EXECUTE){
			if (num_node == -1){
				std::cout << "NODE_COUNT (-n) should be set." << std::endl;
				exit(0);
			}
		}
	}
	else if (app_type == APP_ISLAND_EXECUTE){
		if (factor_count == 0) {
			std::cout << "SCALE_FACTOR (-sf) should be set." << std::endl;
			exit(0);
		}
		if (num_core == -1) {
			std::cout << "CORE_COUNT (-c) should be set." << std::endl;
			exit(0);
		}
		if (num_txn == -1) {
			std::cout << "TXN_COUNT (-t) should be set." << std::endl;
			exit(0);
		}
		if (num_node == -1){
			std::cout << "NODE_COUNT (-n) should be set." << std::endl;
			exit(0);
		}
		if (instance_id == -1){
			std::cout << "INSTANCE_ID (-i) should be set." << std::endl;
			exit(0);
		}
	}
	else if (app_type == APP_SERVER_EXECUTE){
	
	}
}

static void ArgumentsParser(int argc, char *argv[]) {
	if (argc <= 2) {
		PrintUsage();
		exit(0);
	}
	for (int i = 1; i < argc; ++i) {
		if (argv[i][0] != '-') {
			PrintUsage();
			exit(0);
		}
		if (argv[i][1] == 'a') {
			app_type = atoi(&argv[i][2]);
		}
		else if (argv[i][1] == 's' && argv[i][2] == 'f') {
			scale_factors[factor_count] = atof(&argv[i][3]);
			++factor_count;
		}
		else if (argv[i][1] == 'p') {
			dir_name = std::string(&argv[i][2]);
		}
		else if (argv[i][1] == 't') {
			num_txn = atoi(&argv[i][2]);
		}
		else if (argv[i][1] == 'd') {
			dist_ratio = atoi(&argv[i][2]);
		}
		else if (argv[i][1] == 'c') {
			num_core = atoi(&argv[i][2]);
		}
		else if (argv[i][1] == 'n'){
			num_node = atoi(&argv[i][2]);
		}
		else if (argv[i][1] == 'r') {
			replay_type = atoi(&argv[i][2]);
		}
		else if (argv[i][1] == 'i') {
			instance_id = atoi(&argv[i][2]);
		}
		else if (argv[i][1] == 'z') {
			Cavalia::Database::gParamBatchSize = atoi(&argv[i][2]);
		}
		else if (argv[i][1] == 'o') {
			Cavalia::Database::gAdhocRatio = atoi(&argv[i][2]);
		}
		else if (argv[i][1] == 'h') {
			PrintUsage();
			exit(0);
		}
		else {
			PrintUsage();
			exit(0);
		}
	}
	ArgumentsChecker();
}

#endif
