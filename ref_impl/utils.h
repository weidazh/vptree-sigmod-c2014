#ifndef _UTILS_H_
#define _UTILS_H_

#include "common.h"

#define ENABLE_STATIC_MALLOC 1

#define SIZE_BSS (1 /* G */ * 1024 * 1024 * 1024)
extern unsigned char BSS[MAX_THREADS][SIZE_BSS];
extern __thread long offset;

static inline void* static_malloc(size_t size) {
	long local_offset = offset;
	offset += size;
	if (thread_sid >= MAX_THREADS) {
		fprintf(logf, "ERROR BSS thread_sid overflow\n");
		return NULL;
	}
	if (offset >= SIZE_BSS) {
		fprintf(logf, "ERROR BSS overflow\n");
		return NULL;
	}
	// fprintf(logf, "allocating %d\n", size);
	return &BSS[thread_sid][local_offset];
}

static inline void static_free(void* var) {
	// do nothing
}

#if ENABLE_STATIC_MALLOC
#define MALLOC(size) static_malloc(size)
#define FREE(var) static_free(var)
#else
#define MALLOC(size) malloc(size)
#define FREE(var) free(var)
#endif


#define ENABLE_ALLOW_MEM_LEAK 1

#if ENABLE_ALLOW_MEM_LEAK
#define DELETE(x) (void(0))
#else
#define DELETE(x) (delete x)
#endif

#endif
