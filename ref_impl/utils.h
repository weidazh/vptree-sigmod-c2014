#ifndef _UTILS_H_
#define _UTILS_H_

#include "common.h"

#define ENABLE_STATIC_MALLOC 1
#define ENABLE_ALLOW_MEM_LEAK 1

#define SIZE_BSS (1 /* G */ * 1024 * 1024 * 1024)
#if ENABLE_STATIC_MALLOC
extern unsigned char BSS[MAX_THREADS][SIZE_BSS];
extern __thread long offset;

static inline void* static_malloc(size_t size) {
	long local_offset = offset;
	if (local_offset % 64 != 0)
		local_offset = local_offset - local_offset % 64 + 64;
	offset = local_offset + size;
	if (thread_sid >= MAX_THREADS) {
		fprintf(logf, "ERROR BSS thread_sid overflow\n");
		return NULL;
	}
	if (offset >= SIZE_BSS) {
		fprintf(logf, "ERROR BSS overflow\n");
		return NULL;
	}
	if (thread_sid == 0) {
		ASSERT_THREAD(MASTER_THREAD, 0);
	}
	// fprintf(logf, "allocating %d\n", size);
	return &BSS[thread_sid][local_offset];
}

static inline void static_free(void* var) {
	// do nothing
}
#endif

#if ENABLE_STATIC_MALLOC
#define MALLOC(size) static_malloc(size)
#define FREE(var) static_free(var)
#define NEW(type, ...) ({\
		void* __mem = static_malloc(sizeof(type)); \
		new (__mem) type(__VA_ARGS__); \
	})
#else
#define MALLOC(size) malloc(size)
#define FREE(var) free(var)
#define NEW(type, ...) (new type(__VA_ARGS__))
#endif


#if ENABLE_ALLOW_MEM_LEAK
#define DELETE(x) (void(0))
#else
#define DELETE(x) (delete x)
#endif

int GetCPUID();
void StickToCores(int type, int tid, int max);




void FastEditDistanceBuildIndex();
int FastEditDistance(const char* a, int na, const char* b, int nb);
int fastindex_main();
#endif
