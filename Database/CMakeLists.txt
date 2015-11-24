PROJECT (Cavalia)
file (GLOB_RECURSE SRC_LIST "*.cc" "*.cpp")
ADD_LIBRARY (storage_static STATIC ${SRC_LIST})
ADD_LIBRARY (storage_shared SHARED ${SRC_LIST})
#TARGET_LINK_LIBRARIES (storage_shared libserialization.so)
#TARGET_LINK_LIBRARIES (storage_shared libcommunication.so)
TARGET_LINK_LIBRARIES (storage_shared libpapi.so libjemalloc.so libprotobuf.so)
#TARGET_LINK_LIBRARIES (storage_shared libboost_filesystem.so libboost_system.so libboost_thread.so)
SET_TARGET_PROPERTIES (storage_static PROPERTIES OUTPUT_NAME "storage_engine" CLEAN_DIRECT_OUTPUT 1)
SET_TARGET_PROPERTIES (storage_shared PROPERTIES OUTPUT_NAME "storage_engine" CLEAN_DIRECT_OUTPUT 1)
INSTALL (TARGETS storage_static storage_shared
	ARCHIVE DESTINATION bin
	LIBRARY DESTINATION bin
	)
