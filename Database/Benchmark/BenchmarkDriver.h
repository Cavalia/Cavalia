#pragma once
#ifndef __CAVALIA_BENCHMARK_FRAMEWORK_BENCHMARK_DRIVER_H__
#define __CAVALIA_BENCHMARK_FRAMEWORK_BENCHMARK_DRIVER_H__

#include "../Meta/MetaTypes.h"

#define CHECK_DIRECTORY(BenchmarkName, DirName) \
	std::string full_name = DirName + "/" + #BenchmarkName;\
if (boost::filesystem::exists(full_name) == false){ \
	bool rt = boost::filesystem::create_directory(full_name); \
	assert(rt == true); \
}

#define POPULATE_STORAGE(BenchmarkName, DirName) \
	ShareStorageManager storage_manager(DirName+"/"#BenchmarkName"/Checkpoint", false); \
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

#define ENABLE_COMMAND_LOGGER(BenchmarkName, DirName, NumTxn) \
	logger = new CommandLogger(DirName+"/"#BenchmarkName"/", NumTxn);

#define ENABLE_VALUE_LOGGER(BenchmarkName, DirName, NumTxn) \
	logger = new ValueLogger(DirName+"/"#BenchmarkName"/", NumTxn);

////////////////////////////////////////////////////////////
#define SET_SOURCE(BenchmarkName, DirName, NumTxn) \
	BenchmarkName##Source source(DirName+"/"#BenchmarkName"/txn", &io_redirector, &params, NumTxn, RANDOM_SOURCE); \
	source.Start();

#define SET_SOURCE_PARTITION(BenchmarkName, DirName, NumTxn, NumPartition, DistRatio) \
	BenchmarkName##Source source(DirName+"/"#BenchmarkName"/txn", &io_redirector, &params, NumTxn, PARTITION_SOURCE, NumPartition, DistRatio); \
	source.Start();

///////////////////////////////////////////////////////////
#define RELOAD_STORAGE(BenchmarkName, DirName, ThreadSafe) \
	ShareStorageManager storage_manager(DirName+"/"#BenchmarkName"/Checkpoint", ThreadSafe); \
	BenchmarkName##TableInitiator initiator; \
	initiator.Initialize(&storage_manager); \
	storage_manager.ReloadCheckpoint();

// multiple concurrency control types
#define EXECUTE_TRANSACTIONS_CONCURRENT(BenchmarkName, NumCore) \
	BenchmarkName##ConcurrentExecutor executor(&io_redirector, &storage_manager, logger, NumCore); \
	executor.Start();

////////////////////////////////////////////////////////////
#define RELOAD_STORAGE_PARTITION(BenchmarkName, DirName, ThreadSafe) \
	BenchmarkName##ShardStorageManager storage_manager(DirName+"/"#BenchmarkName"/Checkpoint", configure.GetTableLocations(), ThreadSafe); \
	BenchmarkName##TableInitiator initiator; \
	initiator.Initialize(&storage_manager); \
	storage_manager.ReloadCheckpoint();

#define CONFIGURE_HSTORE(BenchmarkName, NumCore, NumNode) \
	BenchmarkName##HStoreConfiguration configure(NumCore, NumNode); \
	configure.MeasureConfiguration();

#define EXECUTE_TRANSACTIONS_HSTORE(BenchmarkName) \
	BenchmarkName##HStoreExecutor executor(&io_redirector, &storage_manager, logger, configure.GetTxnLocation()); \
	executor.Start();

#define CONFIGURE_SITE(BenchmarkName, NumCore, NumNode) \
	BenchmarkName##SiteConfiguration configure(NumCore, NumNode); \
	configure.MeasureConfiguration();

#define EXECUTE_TRANSACTIONS_SITE(BenchmarkName) \
	BenchmarkName##SiteExecutor executor(&io_redirector, &storage_manager, logger, configure.GetTxnLocation()); \
	executor.Start();

////////////////////////////////////////////////////////////
#define COMMAND_REPLAY(BenchmarkName, DirName, NumCore) \
	BenchmarkName##CommandReplayer replayer(DirName+"/"#BenchmarkName"/", &storage_manager, NumCore); \
	replayer.Start();

#define VALUE_REPLAY(BenchmarkName, DirName, NumCore) \
	BenchmarkName##ValueReplayer replayer(DirName+"/"#BenchmarkName"/", &storage_manager, NumCore); \
	replayer.Start();

#endif
