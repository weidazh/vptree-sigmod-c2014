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
	long long start_enqueuing;
	long long total_enqueuing;

	long long total_wait;
	long long total_words_wait;
	long long total_docs_wait;
	long long start_serial;
	long long total_serial;
	long long start_parallel;
	long long total_parallel;
	long long total_indexing;
	long long start_indexing_and_query_adding;
	long long total_indexing_and_query_adding;
	long long total_resultmerging;
};
extern __thread long long thread_total_resultmerging;

extern struct stats stats;
#define MASTER_THREAD 1
#define DOC_WORKER_THREAD 2
#define WORD_SEARCHER_THREAD 3
extern __thread int thread_type;
extern __thread int thread_id;

#define DOC_WORKER_N 24
extern int doc_worker_n;
#define WORD_SEARCHER_N 24
extern int word_searcher_n;

#define ASSERT_THREAD(type, id) \
	if (type != thread_type || id != thread_id) { \
		fprintf(stderr, "%d:%d ERROR not %d:%d %s:L%d\n", \
				thread_type, thread_id,	 \
				type, id, \
				__FILE__, __LINE__); \
	} else {}
#endif
