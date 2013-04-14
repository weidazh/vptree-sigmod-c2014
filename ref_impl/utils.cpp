#include <stdio.h>
#include "utils.h"

unsigned char BSS[MAX_THREADS][SIZE_BSS];
__thread long offset = 0;

#define _GNU_SOURCE
#include <pthread.h>
#include <sys/sysinfo.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/syscall.h>
#include <linux/unistd.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
static int gettid() {
	return syscall(SYS_gettid);
}

int GetCPUID() {
	char filename[128];
	sprintf(filename, "/proc/self/task/%d/stat", gettid()); 
	FILE* f = fopen(filename, "r");
	int pid;
	char procname[128];
	char state[5];
	int d;
	unsigned int u;
	unsigned long lu;
	long ld;
	unsigned long long llu;
	unsigned long vss;
	long rss;
	int prs;

	fscanf(f, "%d %s %s "
			"%*d %*d %*d %*d %*d %*d "
			"%*d %*d %*d %*d %*d %*d "
			"%*d %*d %*d %*d %*d %*d %*d " /*vss*/"%lu " /*rss*/"%ld "
			"%*d %*d %*d %*d %*d %*d %*d %*d %*d %*d %*d %*d %*d %*d " /* prs */"%d %*d %*d %*d %*d %*d",
		&pid, procname, state, &vss, &rss, &prs);
	return prs;
}
#if ENABLE_AFFINITY_SETTING
void StickToCores(int type, int tid, int max) {
	ASSERT_THREAD(thread_type, tid);

	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	if (N_CORES % max != 0) {
		fprintf(stderr, "N_CORES % max != 0\n");
		exit(1);
	}
	int mapping[] = {0, 12, 1, 13, 2, 14, 3, 15, 4, 16, 5, 17,
			 6, 18, 7, 19, 8, 20, 9, 21, 10, 22, 11, 23};
	for (int i = 0; i < N_CORES / max; i++) {
		CPU_SET(mapping[tid * N_CORES / max + i], &cpuset);
	}

	int ret = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
	if (ret < 0) {
		fprintf(stderr, "cannot set affinity\n");
		exit(1);
	}
}
#else
void StickToCores(int type, int tid, int max) {
}
#endif
