#ifndef _COMMON_H_
#define _COMMON_H_

#include <sys/time.h>
static long long GetClockTimeInUS()
{
	struct timeval t2;
	gettimeofday(&t2,NULL);
	return t2.tv_sec*1000000LL+t2.tv_usec;
}

struct stats {
	long long total_wait;
	long long start_serial;
	long long total_serial;
	long long start_parallel;
	long long total_parallel;
	long long total_indexing;
};

extern struct stats stats;
#endif
