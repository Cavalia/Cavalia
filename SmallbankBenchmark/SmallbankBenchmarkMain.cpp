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

#include "SmallbankTableInitiator.h"
#include "SmallbankPopulator.h"
#include "SmallbankSource.h"
#include "SmallbankConcurrentExecutor.h"
#if defined(ST)
#include "SmallbankSerialCommandReplayer.h"
#include "SmallbankValueReplayer.h"
#endif
#if defined(__linux__)
#include "SmallbankShardStorageManager.h"
#include "SmallbankHStoreConfiguration.h"
#include "SmallbankHStoreExecutor.h"
#include "SmallbankSiteConfiguration.h"
#include "SmallbankSiteExecutor.h"
//#include "SmallbankIslandStorageManager.h"
//#include "SmallbankIslandConfiguration.h"
//#include "SmallbankIslandExecutor.h"
#endif
using namespace Cavalia;
using namespace Cavalia::Benchmark::Smallbank;
using namespace Cavalia::Benchmark::Smallbank::Executor;
#if defined(ST)
using namespace Cavalia::Benchmark::Smallbank::Replayer;
#endif

int main(int argc, char *argv[]) {
	ArgumentsParser(argc, argv);
	std::cout << "directory name: " << dir_name << std::endl;
	CHECK_DIRECTORY(Smallbank, dir_name);
	if (app_type == APP_POPULATE) {
		assert(factor_count == 1);
		SmallbankScaleParams params(scale_factors[0], -1);
		POPULATE_STORAGE(Smallbank, dir_name);
		PRINT_STORAGE_STATUS;
		CHECKPOINT_STORAGE;
	}
#if defined(ST)
	else if (app_type == APP_REPLAY) {
		if (replay_type == APP_SERIAL_COMMAND_REPLAY) {
			RELOAD_STORAGE(Smallbank, dir_name, false);
			PRINT_STORAGE_STATUS;
			SERIAL_COMMAND_REPLAY(Smallbank, dir_name, num_core);
			PRINT_STORAGE_STATUS;
		}
		else if (replay_type == APP_VALUE_REPLAY) {
			RELOAD_STORAGE(Smallbank, dir_name, true);
			PRINT_STORAGE_STATUS;
			VALUE_REPLAY(Smallbank, dir_name, num_core);
			PRINT_STORAGE_STATUS;
		}
	}
#endif
	else if (app_type == APP_CC_EXECUTE || app_type == APP_HSTORE_EXECUTE || app_type == APP_SITE_EXECUTE){
		assert(factor_count == 2);
		SmallbankScaleParams params(scale_factors[0], scale_factors[1]);
		BaseLogger *logger = NULL;
		if (app_type == APP_CC_EXECUTE) {
#if defined(COMMAND_LOGGING)
			ENABLE_COMMAND_LOGGER(Smallbank, dir_name, num_core);
#endif
#if defined(VALUE_LOGGING)
			ENABLE_VALUE_LOGGER(Smallbank, dir_name, num_core);
#endif
			IORedirector io_redirector(num_core);
			SET_SOURCE(Smallbank, dir_name, num_txn, dist_ratio);
			//SET_SOURCE_PARTITION(Smallbank, num_txn, num_core, dist_ratio);
			INIT_PROFILERS;
			RELOAD_STORAGE(Smallbank, dir_name, true);
			PRINT_STORAGE_STATUS;
			EXECUTE_TRANSACTIONS_CONCURRENT(Smallbank, num_core);
			PRINT_STORAGE_STATUS;
			REPORT_PROFILERS;
		}
#if defined(__linux__)
		else if (app_type == APP_HSTORE_EXECUTE) {
			size_t total_num_core = num_core * num_node;
			CONFIGURE_HSTORE(Smallbank, num_core, num_node);
			IORedirector io_redirector(total_num_core);
			SET_SOURCE_PARTITION(Smallbank, dir_name, num_txn, dist_ratio, total_num_core);
			RELOAD_STORAGE_PARTITION(Smallbank, dir_name, false);
			PRINT_STORAGE_STATUS;
			EXECUTE_TRANSACTIONS_HSTORE(Smallbank);
			PRINT_STORAGE_STATUS;
		}
		else if (app_type == APP_SITE_EXECUTE) {
			size_t total_num_core = num_core * num_node;
			CONFIGURE_SITE(Smallbank, num_core, num_node);
			IORedirector io_redirector(total_num_core);
			SET_SOURCE_PARTITION(Smallbank, dir_name, num_txn, dist_ratio, num_node);
			RELOAD_STORAGE_PARTITION(Smallbank, dir_name, false);
			PRINT_STORAGE_STATUS;
			EXECUTE_TRANSACTIONS_SITE(Smallbank);
			PRINT_STORAGE_STATUS;
		}
#endif
		delete logger;
		logger = NULL;
	}
	std::cout << "finished everything..." << std::endl;
	return 0;
}

