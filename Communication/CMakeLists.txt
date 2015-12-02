PROJECT (Cavalia)
file (GLOB SRC_LIST "*.cpp")
ADD_LIBRARY (communication_static STATIC ${SRC_LIST})
ADD_LIBRARY (communication_shared SHARED ${SRC_LIST})
TARGET_LINK_LIBRARIES (communication_shared libzmq.so)
SET_TARGET_PROPERTIES (communication_static PROPERTIES OUTPUT_NAME "communication" CLEAN_DIRECT_OUTPUT 1)
SET_TARGET_PROPERTIES (communication_shared PROPERTIES OUTPUT_NAME "communication" CLEAN_DIRECT_OUTPUT 1)
INSTALL (TARGETS communication_static communication_shared
	ARCHIVE DESTINATION bin
	LIBRARY DESTINATION bin
	)
