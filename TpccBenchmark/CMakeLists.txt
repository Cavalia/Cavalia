PROJECT (Cavalia)
file (GLOB SRC_LIST "*.cpp")
ADD_EXECUTABLE (tpcc_benchmark ${SRC_LIST})

TARGET_LINK_LIBRARIES (tpcc_benchmark libjemalloc.so libdatabase.so 
	liblz4.so libcityhash.so libpapi.so libnuma.so libboost_filesystem.so libboost_system.so libboost_date_time.so libboost_thread.so)
TARGET_LINK_LIBRARIES (tpcc_benchmark pthread rt)
INSTALL (TARGETS tpcc_benchmark
	RUNTIME DESTINATION bin
	)
