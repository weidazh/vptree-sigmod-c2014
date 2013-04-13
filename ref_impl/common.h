#ifndef _COMMON_H_
#define _COMMON_H_

#include <sys/time.h>
static inline long long GetClockTimeInUS()
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
	long long total_docs_wait_small;
	long long start_serial;
	long long total_serial;
	long long total_indexing;
	long long start_indexing_and_query_adding;
	long long total_indexing_and_query_adding;
	long long total_master_indexing;
	long long total_resultmerging;
};
extern __thread long long thread_total_resultmerging;

extern struct stats stats;
extern FILE* logf;

#define DOC_WORKER_N 24
extern int doc_worker_n;
#define WORD_SEARCHER_N 24
extern int word_searcher_n;
#define REQ_RING_N 12
extern int req_ring_n;
#define REQ_ENQUEUE_BATCH 4
extern int req_enqueue_batch;
#define MAX_THREADS (1 + DOC_WORKER_N + WORD_SEARCHER_N)

#define MASTER_THREAD 1
#define DOC_WORKER_THREAD 2
#define WORD_SEARCHER_THREAD 3
extern __thread int thread_type;
extern __thread int thread_id;
extern __thread int thread_sid; /* serialized id, master 0, doc worker 1, doc worker 2, etc*/

#include <stdlib.h>
#include <stdio.h>
static inline void setThread(int type, int id) {
	if (type == MASTER_THREAD)
		thread_sid = id;
	else if (type == DOC_WORKER_THREAD)
		thread_sid = id + 1;
	else if (type == WORD_SEARCHER_THREAD)
		thread_sid = 1 + doc_worker_n + id;
	else {
		fprintf(logf, "Unknwon thread type %d\n", type);
		exit(1);
	}
	thread_type = type;
	thread_id = id;
	// fprintf(stderr, "New thread %d:%d:%d\n", thread_type, thread_id, thread_sid);
}
#define PHRASE_SERIAL 1 /* Input/Output */ 
#define PHRASE_INDEX 2
#define PHRASE_ENQUEUE 3
#define PHRASE_WAIT_WORDS 4
#define PHRASE_WAIT_DOCS 5
extern int phrase;
extern int hku;

#define ASSERT_PHRASE(asserted_phrase) \
	if (phrase != (asserted_phrase)) { \
		fprintf(stderr, "%d:%d-%d ERROR not in phrase %d %s:L%d\n", \
				thread_type, thread_id, phrase, \
				asserted_phrase, __FILE__, __LINE__); \
	}

#define ASSERT_THREAD(type, id) \
	if ((type) != thread_type || (id) != thread_id) { \
		fprintf(stderr, "%d:%d ERROR not %d:%d %s:L%d\n", \
				thread_type, thread_id,	 \
				(type), (id), \
				__FILE__, __LINE__); \
	} else {}
#endif
