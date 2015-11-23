#include <iostream>

#include <boost/thread.hpp>
#include <boost/filesystem.hpp>

#include <Profilers.h>

#include <IORedirector.h>

#include <BaseLogger.h>

#include <BenchmarkDriver.h>
#include <BenchmarkArguments.h>
#include <ShareStorageManager.h>
#include <ShardStorageManager.h>

#include "TpccTableInitiator.h"
#include "TpccPopulator.h"

#include "TpccSource.h"

#include "TpccConcurrentExecutor.h"

#if defined(FLOW)
#include <FlowExecutionProfiler.h>
#include "TpccCentralFlowConfiguration.h"
#include "TpccCentralFlowExecutor.h"
#endif
#if defined(CHOP)
#include "TpccChopExecutor.h"
#endif

#include "TpccShardStorageManager.h"

#include "TpccHStoreConfiguration.h"
#include "TpccHStoreExecutor.h"

#include "TpccSiteConfiguration.h"
#include "TpccSiteExecutor.h"

using namespace Cavalia;
using namespace Cavalia::Benchmark::Tpcc;
using namespace Cavalia::Benchmark::Tpcc::Executor;

int main(int argc, char *argv[]) {
	ArgumentsParser(argc, argv);
	CHECK_DIRECTORY(Tpcc);
	if (app_type == APP_POPULATE) {
		assert(factor_count == 2);
		TpccScaleParams params((int)(scale_factors[0]), scale_factors[1]);
		POPULATE_STORAGE(Tpcc);
		PRINT_STORAGE_STATUS;
		CHECKPOINT_STORAGE;
	}
	else {
		assert(factor_count == 2);
		TpccScaleParams params((int)(scale_factors[0]), scale_factors[1]);
		BaseLogger *logger = NULL;
		if (app_type == APP_CC_EXECUTE) {
#if defined(VALUE_LOGGING) || defined(COMMAND_LOGGING)
			ENABLE_MEMORY_LOGGER(num_core);
#endif
			IORedirector io_redirector(num_core);
			//SET_SOURCE(Tpcc, num_txn);
			SET_SOURCE_PARTITION(Tpcc, num_txn, (int)(scale_factors[0]), dist_ratio);
			INIT_PROFILERS;
			RELOAD_STORAGE(Tpcc, false);
			PRINT_STORAGE_STATUS;
			EXECUTE_TRANSACTIONS_CONCURRENT(Tpcc, num_core);
			PRINT_STORAGE_STATUS;
			REPORT_PROFILERS;
		}
		else if (app_type == APP_HSTORE_EXECUTE) {
			// number of threads should be equal to number of warehouses.
			size_t total_num_core = num_core * num_node;
			assert(total_num_core == int(scale_factors[0]));
			CONFIGURE_HSTORE(Tpcc, num_core, num_node);
			IORedirector io_redirector(total_num_core);
			SET_SOURCE_PARTITION(Tpcc, num_txn, total_num_core, dist_ratio);
			RELOAD_STORAGE_PARTITION(Tpcc, false);
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
			SET_SOURCE_PARTITION(Tpcc, num_txn, num_node, dist_ratio);
			RELOAD_STORAGE_PARTITION(Tpcc, true);
			PRINT_STORAGE_STATUS;
			EXECUTE_TRANSACTIONS_SITE(Tpcc);
			PRINT_STORAGE_STATUS;
		}
		else if (app_type == APP_FLOW_EXECUTE) {
#if defined(FLOW)
			size_t total_num_core = num_core * num_node;
			IORedirector io_redirector(1);
			SET_SOURCE(Tpcc, num_txn);
			INIT_FLOW_EXECUTION_TIME_PROFILER;
			CONFIGURE_CENTRAL_FLOW(Tpcc, num_core, num_node);
			RELOAD_STORAGE_FLOW(Tpcc);
			PRINT_STORAGE_STATUS;
			EXECUTE_TRANSACTIONS_CENTRAL_FLOW(Tpcc, total_num_core);
			PRINT_STORAGE_STATUS;
			REPORT_FLOW_EXECUTION_TIME_PROFILER(total_num_core);
#endif
		}
		else if (app_type == APP_CHOP_EXECUTE){
#if defined(CHOP)
			IORedirector io_redirector(num_core);
			SET_SOURCE_PARTITION(Tpcc, num_txn, (int)(scale_factors[0]), dist_ratio);
			INIT_PROFILERS;
			RELOAD_STORAGE(Tpcc, true);
			PRINT_STORAGE_STATUS;
			EXECUTE_TRANSACTIONS_CHOP(Tpcc, num_core);
			PRINT_STORAGE_STATUS;
			REPORT_PROFILERS;
#endif
		}
		delete logger;
		logger = NULL;
	}
	std::cout << "finished everything..." << std::endl;
	return 0;
}

