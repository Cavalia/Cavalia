#pragma once
#ifndef __COMMON_ALLOCATOR_HELPER_H__
#define __COMMON_ALLOCATOR_HELPER_H__

#include <cstdlib>
#include <cstring>
#include <cassert>
#if defined(NUMA)
#include <numa.h>
#endif

class MemAllocator{
public:
	static char* Alloc(const size_t &size){
		return (char*)malloc(size);
	}

	static void Free(char *ptr){
		free(ptr);
	}

	static char* AllocNode(const size_t &size, const size_t &numa_node_id){
#if defined(NUMA)
		return (char*)numa_alloc_onnode(size, numa_node_id);
#else
		return (char*)malloc(size);
#endif
	}

	 static void FreeNode(char *ptr, const size_t &size){
#if defined(NUMA)
		numa_free(ptr, size);
#else
		free(ptr);
#endif
	}

	static char* AllocLocal(const size_t &size){
#if defined(NUMA)
		return (char*)numa_alloc_local(size);
#else
		return (char*)malloc(size);
#endif
	}

	static void FreeLocal(char *ptr, const size_t &size){
#if defined(NUMA)
		numa_free(ptr, size);
#else
		free(ptr);
#endif
	}

private:
	MemAllocator(const MemAllocator&);
	MemAllocator& operator=(const MemAllocator&);
};

#endif
