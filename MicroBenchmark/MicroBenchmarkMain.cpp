#include <iostream>

#include <boost/filesystem.hpp>

#include <Benchmark/BenchmarkDriver.h>
#include <Benchmark/BenchmarkArguments.h>

#include <Profiler/Profilers.h>
#include <Redirector/IORedirector.h>
#include <Logger/CommandLogger.h>
#include <Logger/ValueLogger.h>
#include <Storage/ShareStorageManager.h>

#include "MicroTableInitiator.h"
#include "MicroPopulator.h"
#include "MicroSource.h"
#include "MicroConcurrentExecutor.h"

using namespace Cavalia;
using namespace Cavalia::Benchmark::Micro;
using namespace Cavalia::Benchmark::Micro::Executor;

int main(int argc, char *argv[]) {
	ArgumentsParser(argc, argv);
	std::cout << "directory name: " << dir_name << std::endl;
	CHECK_DIRECTORY(Micro, dir_name);
	if (app_type == APP_POPULATE) {
		assert(factor_count == 1);
		MicroScaleParams params(scale_factors[0], -1);
		POPULATE_STORAGE(Micro, dir_name);
		PRINT_STORAGE_STATUS;
		CHECKPOINT_STORAGE;
	}
	else if (app_type == APP_CC_EXECUTE){
		assert(factor_count == 2);
		MicroScaleParams params(scale_factors[0], scale_factors[1]);
		BaseLogger *logger = NULL;
#if defined(COMMAND_LOGGING)
		ENABLE_COMMAND_LOGGER(Micro, dir_name, num_core);
#endif
#if defined(VALUE_LOGGING)
		ENABLE_VALUE_LOGGER(Micro, dir_name, num_core);
#endif
		IORedirector io_redirector(num_core);
		SET_SOURCE(Micro, dir_name, num_txn, dist_ratio);
		INIT_PROFILERS;
		RELOAD_STORAGE(Micro, dir_name, true);
		PRINT_STORAGE_STATUS;
		EXECUTE_TRANSACTIONS_CONCURRENT(Micro, num_core);
		PRINT_STORAGE_STATUS;
		REPORT_PROFILERS;
		delete logger;
		logger = NULL;
	}
	std::cout << "finished everything..." << std::endl;
	return 0;
}

