PROJECT (Cavalia)
file (GLOB_RECURSE SRC_LIST "*.cc" "*.cpp")
ADD_LIBRARY (database_static STATIC ${SRC_LIST})
ADD_LIBRARY (database_shared SHARED ${SRC_LIST})
#TARGET_LINK_LIBRARIES (database_shared libserialization.so)
#TARGET_LINK_LIBRARIES (database_shared libcommunication.so)
TARGET_LINK_LIBRARIES (database_shared liblz4.so)
TARGET_LINK_LIBRARIES (database_shared libpapi.so libjemalloc.so)
#TARGET_LINK_LIBRARIES (database_shared libboost_filesystem.so libboost_system.so libboost_thread.so)
SET_TARGET_PROPERTIES (database_static PROPERTIES OUTPUT_NAME "database" CLEAN_DIRECT_OUTPUT 1)
SET_TARGET_PROPERTIES (database_shared PROPERTIES OUTPUT_NAME "database" CLEAN_DIRECT_OUTPUT 1)
INSTALL (TARGETS database_static database_shared
	ARCHIVE DESTINATION bin
	LIBRARY DESTINATION bin
	)
