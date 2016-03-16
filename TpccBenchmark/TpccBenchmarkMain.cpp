#include <iostream>

#include <boost/filesystem.hpp>

#include <Benchmark/BenchmarkDriver.h>
#include <Benchmark/BenchmarkArguments.h>
#include <Benchmark/BenchmarkConfigParser.h>

#include <Profiler/Profilers.h>
#include <Redirector/IORedirector.h>
#include <Logger/CommandLogger.h>
#include <Logger/ValueLogger.h>
#include <Storage/ShareStorageManager.h>
#include <Storage/ShardStorageManager.h>

#include "TpccTableInitiator.h"
#include "TpccPopulator.h"
#include "TpccSource.h"
#include "TpccConcurrentExecutor.h"
#if defined(ST)
#include "TpccSerialCommandReplayer.h"
#include "TpccValueReplayer.h"
#endif
#if defined(NUMA)
#include "TpccShardStorageManager.h"
#include "TpccHStoreConfiguration.h"
#include "TpccHStoreExecutor.h"
#include "TpccSiteConfiguration.h"
#include "TpccSiteExecutor.h"
#include "TpccIslandStorageManager.h"
#include "TpccIslandConfiguration.h"
#include "TpccIslandExecutor.h"
#endif

using namespace Cavalia;
using namespace Cavalia::Benchmark::Tpcc;
using namespace Cavalia::Benchmark::Tpcc::Executor;
#if defined(ST)
using namespace Cavalia::Benchmark::Tpcc::Replayer;
#endif

int main(int argc, char *argv[]) {
	ArgumentsParser(argc, argv);
	std::cout << "directory name: " << dir_name << std::endl;
	CHECK_DIRECTORY(Tpcc, dir_name);
	if (app_type == APP_POPULATE) {
		assert(factor_count == 2);
		TpccScaleParams params((int)(scale_factors[0]), scale_factors[1]);
		POPULATE_STORAGE(Tpcc, dir_name);
		PRINT_STORAGE_STATUS;
		CHECKPOINT_STORAGE;
	}
#if defined(ST)
	else if (app_type == APP_REPLAY) {
		if (replay_type == APP_SERIAL_COMMAND_REPLAY) {
			RELOAD_STORAGE(Tpcc, dir_name, false);
			PRINT_STORAGE_STATUS;
			SERIAL_COMMAND_REPLAY(Tpcc, dir_name, num_core);
			PRINT_STORAGE_STATUS;
		}
		else if (replay_type == APP_VALUE_REPLAY) {
			RELOAD_STORAGE(Tpcc, dir_name, true);
			PRINT_STORAGE_STATUS;
			VALUE_REPLAY(Tpcc, dir_name, num_core);
			PRINT_STORAGE_STATUS;
		}
	}
#endif
	else if (app_type == APP_CC_EXECUTE || app_type == APP_HSTORE_EXECUTE || app_type == APP_SITE_EXECUTE){
		assert(factor_count == 2);
		TpccScaleParams params((int)(scale_factors[0]), scale_factors[1]);
		BaseLogger *logger = NULL;
		if (app_type == APP_CC_EXECUTE) {
#if defined(COMMAND_LOGGING)
			ENABLE_COMMAND_LOGGER(Tpcc, dir_name, num_core);
#endif
#if defined(VALUE_LOGGING)
			ENABLE_VALUE_LOGGER(Tpcc, dir_name, num_core);
#endif
			IORedirector io_redirector(num_core);
			SET_SOURCE(Tpcc, dir_name, num_txn, dist_ratio);
			//SET_SOURCE_PARTITION(Tpcc, dir_name, num_txn, dist_ratio, (int)(scale_factors[0]));
			INIT_PROFILERS;
			RELOAD_STORAGE(Tpcc, dir_name, true);
			PRINT_STORAGE_STATUS;
			EXECUTE_TRANSACTIONS_CONCURRENT(Tpcc, num_core);
			PRINT_STORAGE_STATUS;
			REPORT_PROFILERS;
		}
#if defined(NUMA)
		else if (app_type == APP_HSTORE_EXECUTE) {
			// number of threads should be equal to number of warehouses.
			size_t total_num_core = num_core * num_node;
			assert(total_num_core == int(scale_factors[0]));
			CONFIGURE_HSTORE(Tpcc, num_core, num_node);
			IORedirector io_redirector(total_num_core);
			SET_SOURCE_PARTITION(Tpcc, dir_name, num_txn, dist_ratio, total_num_core);
			RELOAD_STORAGE_PARTITION(Tpcc, dir_name, false);
			PRINT_STORAGE_STATUS;
			EXECUTE_TRANSACTIONS_HSTORE(Tpcc);
			PRINT_STORAGE_STATUS;
		}
		else if (app_type == APP_SITE_EXECUTE) {
			// number of warehouses must be larger than number of numa nodes.
			size_t total_num_core = num_core * num_node;
			assert(num_node <= int(scale_factors[0]));
			CONFIGURE_SITE(Tpcc, num_core, num_node);
			IORedirector io_redirector(total_num_core);
			SET_SOURCE_PARTITION(Tpcc, dir_name, num_txn, dist_ratio, num_node);
			RELOAD_STORAGE_PARTITION(Tpcc, dir_name, true);
			PRINT_STORAGE_STATUS;
			EXECUTE_TRANSACTIONS_SITE(Tpcc);
			PRINT_STORAGE_STATUS;
		}
#endif
		delete logger;
		logger = NULL;
	}
#if defined(NUMA)
	else if (app_type == APP_ISLAND_EXECUTE){
		assert(factor_count == 2);
		TpccScaleParams params((int)(scale_factors[0]), scale_factors[1]);
		BaseLogger *logger = NULL;
		size_t total_num_core = num_core * num_node;
		assert(num_node <= int(scale_factors[0]));
		CONFIGURE_ISLAND(Tpcc, num_core, num_node, instance_id);
		IORedirector io_redirector(total_num_core);
		SET_SOURCE_SELECT(Tpcc, dir_name, num_txn, dist_ratio, total_num_core, instance_id);
		RELOAD_STORAGE_SELECT(Tpcc, dir_name, true);
		PRINT_STORAGE_STATUS;
		delete logger;
		logger = NULL;
	}
	else if (app_type == APP_SERVER_EXECUTE){
		assert(factor_count == 2);
		TpccScaleParams params((int)(scale_factors[0]), scale_factors[1]);
		BenchmarkConfigParser conf_parser;
		conf_parser.LogConfigFile(true);
		if (instance_id == -1){
			// this is client.
		}
		else{
			// reload storage partition.
			//ShareStorageManager storage_manager(DirName + "/"#BenchmarkName"/Checkpoint", true);
			//TpccTableInitiator initiator;
			//initiator.Initialize(&storage_manager);
			//storage_manager.ReloadCheckpoint();
			// execute transaction.
		}
	}
#endif
	std::cout << "finished everything..." << std::endl;
	return 0;
}

