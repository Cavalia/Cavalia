PROJECT (Cavalia)
file (GLOB SRC_LIST "*.cpp")
ADD_EXECUTABLE (smallbank_benchmark ${SRC_LIST})

TARGET_LINK_LIBRARIES (smallbank_benchmark libjemalloc.so libdatabase.so 
	liblz4.so libcityhash.so libpapi.so libnuma.so libboost_filesystem.so libboost_system.so libboost_date_time.so libboost_thread.so)
TARGET_LINK_LIBRARIES (smallbank_benchmark pthread rt)
INSTALL (TARGETS smallbank_benchmark
	RUNTIME DESTINATION bin
	)
