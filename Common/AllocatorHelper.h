#pragma once
#ifndef __COMMON_ALLOCATOR_HELPER_H__
#define __COMMON_ALLOCATOR_HELPER_H__

#include <cstdlib>
#include <cstring>
#include <cassert>
#include <mutex>
#if defined(NUMA)
#include <numa.h>
#endif
#if defined(THREAD_ALLOC)
#include <pthread.h>
#endif

#define MEM_ALLIGN 8
static const size_t kBlockSizes[] = { 32, 64, 256, 1024 };
static const size_t kBlockNums[] = {81920 * 8, 81920 * 8, 81920, 10240 };
static const size_t kSizeNum = 4;
#if defined(THREAD_ALLOC)
// chain
struct FreeBlock{
	size_t size_;
	FreeBlock *next_;
};

struct PidCid{
	PidCid(){
		pid_ = 0;
		tid_ = 0;
		cid_ = 0;
	}
	pthread_t pid_;
	int tid_;
	int cid_;
};

class Arena{
public:
	Arena(){}
	~Arena(){}

	void Init(const size_t &node_id, const size_t &size, const size_t &size_id){
		block_size_ = size;
		size_id_ = size_id;
		size_in_buffer_ = block_size_ * kBlockNums[size_id_];
#if defined(NUMA)
		buffer_ = (char*)numa_alloc_onnode(size_in_buffer_, node_id);
#else
		buffer_ = (char*)malloc(size_in_buffer_);
#endif
		memset(buffer_, 0, size_in_buffer_);
		free_block_head_ = NULL;
	}

	char* Alloc(){
		FreeBlock *block;
		// allocate from buffer.
		if (free_block_head_ == NULL){
			// block size: the real size that is allocated to the user.
			// sizeof(FreeBlock): meta data size.
			size_t size = (block_size_ + sizeof(FreeBlock)+(MEM_ALLIGN - 1)) & ~(MEM_ALLIGN - 1);
			if (size_in_buffer_ < size){
				size_in_buffer_ = block_size_ * kBlockNums[size_id_];
#if defined(NUMA)
				buffer_ = (char*)numa_alloc_local(size_in_buffer_);
#else
				buffer_ = (char*)malloc(size_in_buffer_);
#endif
			}
			block = (FreeBlock*)buffer_;
			block->size_ = block_size_;
			size_in_buffer_ -= size;
			buffer_ = buffer_ + size;
		}
		// if exists block in the free block list.
		else{
			block = free_block_head_;
			free_block_head_ = free_block_head_->next_;
		}
		// size information is recorded in the head of the free block.
		return (char*)(block) + sizeof(FreeBlock);
	}

	void Free(char *ptr){
		FreeBlock *block = (FreeBlock*)(ptr - sizeof(FreeBlock));
		block->next_ = free_block_head_;
		free_block_head_ = block;
	}

public:
	char *buffer_;
	size_t size_in_buffer_;
	size_t block_size_;
	size_t size_id_;
	FreeBlock *free_block_head_;
};
#endif

class MemAllocator{
public:
	MemAllocator(){}
	~MemAllocator(){}

	void Init(const size_t &thread_num){
#if defined(THREAD_ALLOC)
		int thread_size = 0;
		for (size_t i = 0; i < kSizeNum; ++i){
			thread_size += kBlockSizes[i] * kBlockNums[i];
		}
		thread_size = thread_size / 1024 / 1024;
		printf("per-thread allocate size=%d MB, total size=%d MB\n", thread_size, thread_size * thread_num);
		thread_cnt_ = thread_num;
		// for each thread, create an arena.
		arenas_ = new Arena*[thread_cnt_];
		bucket_cnt_ = thread_cnt_ * 4 + 1;
		pid_tid_ = new PidCid[bucket_cnt_];
#endif
	}

	void RegisterThread(const size_t &thread_id, const size_t &core_id){
#if defined(THREAD_ALLOC)
		register_lock_.lock();
		pthread_t pid = pthread_self();
		int entry = pid % bucket_cnt_;
		while (pid_tid_[entry].pid_ != 0){
			entry = (entry + 1) % bucket_cnt_;
		}
		pid_tid_[entry].pid_ = pid;
		pid_tid_[entry].tid_ = thread_id;
		pid_tid_[entry].cid_ = core_id;
		register_lock_.unlock();

		int node_id = numa_node_of_cpu(core_id);
#if defined(NUMA)
		arenas_[thread_id] = (Arena*)numa_alloc_onnode(sizeof(Arena)*kSizeNum, node_id);
		for (size_t j = 0; j < kSizeNum; ++j){
			new(&(arenas_[thread_id][j]))Arena();
		}
#else
		arenas_[thread_id] = new Arena[kSizeNum];
#endif
		for (size_t j = 0; j < kSizeNum; ++j){
			arenas_[thread_id][j].Init(node_id, kBlockSizes[j], j);
		}
#endif
	}

#if defined(THREAD_ALLOC)
	char* Alloc(const size_t &size){
		assert(size <= kBlockSizes[kSizeNum - 1]);
		return arenas_[GetThreadId()][GetSizeId(size)].Alloc();
	}

	void Free(char *ptr){
		FreeBlock *block = (FreeBlock*)(ptr - sizeof(FreeBlock));
		arenas_[GetThreadId()][GetSizeId(block->size_)].Free(ptr);
	}
#else
	char* Alloc(const size_t &size){
		return (char*)malloc(size);
	}

	void Free(char *ptr){
		free(ptr);
	}
#endif

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
#if defined(THREAD_ALLOC)
	int GetThreadId(){
		pthread_t pid = pthread_self();
		int entry = pid % bucket_cnt_;
		while (pid_tid_[entry].pid_ != pid){
			assert (pid_tid_[entry].pid_ != 0);
			entry = (entry + 1) % bucket_cnt_;
		}
		return pid_tid_[entry].tid_;
	}
	
	size_t GetSizeId(const size_t &size){
		for (size_t i = 0; i < kSizeNum - 1; ++i){
			if (size <= kBlockSizes[i]){
				return i;
			}
		}
		return kSizeNum - 1;
	}
#endif

private:
	MemAllocator(const MemAllocator&);
	MemAllocator& operator=(const MemAllocator&);

private:
#if defined(THREAD_ALLOC)
	size_t thread_cnt_;
	Arena **arenas_;
	size_t bucket_cnt_;
	PidCid *pid_tid_;
	std::mutex register_lock_;
#endif
};

#endif
