#pragma once
#ifndef __CAVALIA_DATABASE_THREAD_LOG_BUFFER_H__
#define __CAVALIA_DATABASE_THREAD_LOG_BUFFER_H__

#include <cstdint>

namespace Cavalia{
	namespace Database{
		struct ThreadLogBuffer{
#if !defined(COMPRESSION)
			ThreadLogBuffer(char *buffer_ptr){
				buffer_ptr_ = buffer_ptr;
				buffer_offset_ = 0;
				last_epoch_ = -1;
			}
#else
			ThreadLogBuffer(char *buffer_ptr, char *compressed_buffer_ptr){
				buffer_ptr_ = buffer_ptr;
				buffer_offset_ = 0;
				last_epoch_ = -1;

				compressed_buffer_ptr_ = compressed_buffer_ptr;
			}
#endif
			char *buffer_ptr_;
			size_t buffer_offset_;
			uint64_t last_epoch_;
#if defined(COMPRESSION)
			char *compressed_buffer_ptr_;
#endif
		};
	}
}

#endif