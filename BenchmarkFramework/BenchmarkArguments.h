#pragma once
#ifndef __CAVALIA_BENCHMARK_FRAMEWORK_BENCHMARK_ARGUMENTS_H__
#define __CAVALIA_BENCHMARK_FRAMEWORK_BENCHMARK_ARGUMENTS_H__

#include <iostream>
#include <cassert>
#include <Meta/MetaTypes.h>

enum AppType { APP_POPULATE, APP_REPLAY, APP_CC_EXECUTE, APP_FLOW_EXECUTE, APP_HSTORE_EXECUTE, APP_SITE_EXECUTE, APP_CHOP_EXECUTE, kAppSize };
enum AppReplayType { APP_SERIAL_REPLAY };

static int app_type = -1;
static double scale_factors[2] = { -1, -1 };
static int factor_count = 0;
static int dist_ratio = 1;
static int num_txn = -1; // default: use disk input redirector
static int num_core = -1; // in site execution, num_core stands for core counts in a single node.
static int num_node = -1; // only used in site execution.
static int num_stage = 1; // default: 1 stage
static int replay_type = 0; // default: serial replay

static void PrintUsage() {
	std::cout << "==========[USAGE]==========" << std::endl;
	std::cout << "\t-aINT: APP_TYPE (0: POPULATE, 1: REPLAY, 2: CC_EXECUTE, 3: FLOW_EXECUTE, 4: HSTORE_EXECUTE, 5 : SITE_EXECUTE)" << std::endl;
	std::cout << "\t-sfDOUBLE: SCALE_FACTOR" << std::endl;
	std::cout << "\t-tINT: TXN_COUNT" << std::endl;
	std::cout << "\t-dINT: DIST_RATIO" << std::endl;
	std::cout << "\t-zINT: BATCH_SIZE" << std::endl;
	std::cout << "\t-cINT: CORE_COUNT" << std::endl;
	std::cout << "\t-nINT: NODE_COUNT" << std::endl;
	std::cout << "\t-sINT: STAGE_COUNT" << std::endl;
	std::cout << "\t-rINT: REPLAY_TYPE (0: SERIAL [DEFAULT])" << std::endl;
	std::cout << "===========================" << std::endl;
	std::cout << "==========[EXAMPLES]==========" << std::endl;
	std::cout << "<POPULATE> Benchmark -a0 -sf10 -sf100" << std::endl;
	//std::cout << "<REPLAY> Benchmark -a1 -sf10 -sf100 [-r2] [-s10] [-c32]" << std::endl;
	std::cout << "<CC_EXECUTE> Benchmark -a2 -sf10 -sf100 -t100000 -c32" << std::endl;
	std::cout << "<FLOW_EXECUTE> Benchmark -a3 -sf10 -sf100 -t100000 -c4 -n5" << std::endl;
	std::cout << "<HSTORE_EXECUTE> Benchmark -a4 -sf10 -sf100 -t100000 -c4 -n5 -d20" << std::endl;
	std::cout << "<SITE_EXECUTE> Benchmark -a5 -sf10 -sf100 -t100000 -c4 -n5 -d20" << std::endl;
	std::cout << "<CHOP_EXECUTE> Benchmark -a6 -sf10 -sf100 -t100000 -c32" << std::endl;
	std::cout << "==============================" << std::endl;
}

static void ArgumentsChecker() {
	if (app_type < 0 || app_type > kAppSize) {
		std::cout << "APP_TYPE (-a) should be in [0-" << kAppSize << "]." << std::endl;
		exit(0);
	}
	if (replay_type < 0 || replay_type > 3) {
		std::cout << "REPLAY_TYPE (-r) should be in [0-1]." << std::endl;
		exit(0);
	}
	else if (app_type == APP_POPULATE) {
		if (factor_count == 0) {
			std::cout << "SCALE_FACTOR (-sf) should be set." << std::endl;
			exit(0);
		}
	}
	else if (app_type == APP_REPLAY) {

	}
	else {
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
		if (app_type == APP_SITE_EXECUTE || app_type == APP_HSTORE_EXECUTE || app_type == APP_FLOW_EXECUTE){
			if (num_node == -1){
			std::cout << "NODE_COUNT (-n) should be set." << std::endl;
			exit(0);
			
			}
		}
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
		else if (argv[i][1] == 's') {
			num_stage = atoi(&argv[i][2]);
		}
		else if (argv[i][1] == 'z') {
			Cavalia::Database::gTupleBatchSize = atoi(&argv[i][2]);
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
