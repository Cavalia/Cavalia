#pragma once
#ifndef __CAVALIA_BENCHMARK_FRAMEWORK_BENCHMARK_DRIVER_H__
#define __CAVALIA_BENCHMARK_FRAMEWORK_BENCHMARK_DRIVER_H__

#include "../Meta/MetaTypes.h"

#define CHECK_DIRECTORY(BenchmarkName) \
if (boost::filesystem::exists(#BenchmarkName) == false){ \
	bool rt = boost::filesystem::create_directory(#BenchmarkName); \
	assert(rt == true); \
}

#define POPULATE_STORAGE(BenchmarkName) \
	ShareStorageManager storage_manager(#BenchmarkName"/Checkpoint", false); \
	BenchmarkName##TableInitiator initiator; \
	initiator.Initialize(&storage_manager); \
	BenchmarkName##Populator populator(&params, &storage_manager); \
	populator.Start();

#define CHECKPOINT_STORAGE \
	storage_manager.SaveCheckpoint();

#define DROP_STORAGE \
	storage_manager.DropAllTables();

#define PRINT_STORAGE_STATUS \
	std::cout << storage_manager.GetStatisticsString() << std::endl;

#define ENABLE_LOGGER(BenchmarkName, LogType) \
	logger = new LogType##Logger(#BenchmarkName"/"#LogType"Logging");

#define ENABLE_MEMORY_LOGGER(NumThread) \
	logger = new BaseLogger(NumThread);

#define DISABLE_LOGGER \
	logger = new NullLogger();

////////////////////////////////////////////////////////////
#define SET_SOURCE(BenchmarkName, NumTxn) \
	BenchmarkName##Source source(#BenchmarkName"/txn", &io_redirector, &params, NumTxn, RANDOM_SOURCE); \
	source.Start();

#define SET_SOURCE_PARTITION(BenchmarkName, NumTxn, NumPartition, DistRatio) \
	BenchmarkName##Source source(#BenchmarkName"/txn", &io_redirector, &params, NumTxn, PARTITION_SOURCE, NumPartition, DistRatio); \
	source.Start();

///////////////////////////////////////////////////////////
#define RELOAD_STORAGE(BenchmarkName, ThreadSafe) \
	ShareStorageManager storage_manager(#BenchmarkName"/Checkpoint", ThreadSafe); \
	BenchmarkName##TableInitiator initiator; \
	initiator.Initialize(&storage_manager); \
	storage_manager.ReloadCheckpoint();

// multiple concurrency control types
#define EXECUTE_TRANSACTIONS_CONCURRENT(BenchmarkName, NumThread) \
	BenchmarkName##ConcurrentExecutor executor(&io_redirector, &storage_manager, logger, NumThread); \
	executor.Start();

////////////////////////////////////////////////////////////
#define RELOAD_STORAGE_PARTITION(BenchmarkName, ThreadSafe) \
	BenchmarkName##ShardStorageManager storage_manager(#BenchmarkName"/Checkpoint", configure.GetTableLocations(), ThreadSafe); \
	BenchmarkName##TableInitiator initiator; \
	initiator.Initialize(&storage_manager); \
	storage_manager.ReloadCheckpoint();

#define CONFIGURE_HSTORE(BenchmarkName, NumThread, NumNode) \
	BenchmarkName##HStoreConfiguration configure(NumThread, NumNode); \
	configure.MeasureConfiguration();

#define EXECUTE_TRANSACTIONS_HSTORE(BenchmarkName) \
	BenchmarkName##HStoreExecutor executor(&io_redirector, &storage_manager, logger, configure.GetTxnLocation()); \
	executor.Start();

#define CONFIGURE_SITE(BenchmarkName, NumThread, NumNode) \
	BenchmarkName##SiteConfiguration configure(NumThread, NumNode); \
	configure.MeasureConfiguration();

#define EXECUTE_TRANSACTIONS_SITE(BenchmarkName) \
	BenchmarkName##SiteExecutor executor(&io_redirector, &storage_manager, logger, configure.GetTxnLocation()); \
	executor.Start();

////////////////////////////////////////////////////////////
#define REPLAY_TRANSACTIONS(BenchmarkName) \
	BenchmarkName##SerialReplayer replayer(#BenchmarkName"/CommandLogging", &storage_manager); \
	replayer.Start();

#endif
