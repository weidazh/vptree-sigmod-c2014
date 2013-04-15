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
	long long total_feed;
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
#define WORD_FEEDER_N 12
#define WORD_WAITER_N WORD_FEEDER_N
extern int word_feeder_n;
#define word_waiter_n word_feeder_n
#define INDEXER_N 24
extern int indexer_n;
#define REQ_RING_N 12
extern int req_ring_n;
#define ENABLE_LEN_AWARE_REQRING 1
#define ENABLE_AFFINITY_SETTING 1
#define ENABLE_MYSTRING 1

#define REQ_RING_POLICY_LEN_MOD 1
#define REQ_RING_POLICY_ROUND_ROBIN 2
#if ENABLE_LEN_AWARE_REQRING
#define REQ_RING_POLICY REQ_RING_POLICY_LEN_MOD
#else
#define REQ_RING_POLICY REQ_RING_POLICY_ROUND_ROBIN
#endif

#define REQ_ENQUEUE_BATCH 1
extern int req_enqueue_batch;
#define MAX_THREADS (1 + DOC_WORKER_N + WORD_SEARCHER_N + WORD_WAITER_N + INDEXER_N)

#if ENABLE_LEN_AWARE_REQRING && REQ_ENQUEUE_BATCH != 1
#error "ENABLE_LEN_AWARE_REQRING && REQ_ENQUEUE_BATCH != 1"
#endif

#define N_CORES 24

#define MASTER_THREAD 1
#define DOC_WORKER_THREAD 2
#define WORD_SEARCHER_THREAD 3
// #define WORD_FEEDER_THREAD 4
#define WORD_WAITER_THREAD 5
#define INDEXER_THREAD 6
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
	// else if (type == WORD_FEEDER_THREAD)
	// 	thread_sid = 1 + doc_worker_n + word_searcher_n + id;
	else if (type == WORD_WAITER_THREAD)
		thread_sid = 1 + doc_worker_n + word_searcher_n + id;
	else if (type == INDEXER_THREAD)
		thread_sid = 1 + doc_worker_n + word_searcher_n + word_waiter_n + id;
	else {
		fprintf(logf, "Unknwon thread type %d\n", type);
		exit(1);
	}
	thread_type = type;
	thread_id = id;
	// fprintf(stderr, "New thread %d:%d:%d\n", thread_type, thread_id, thread_sid);
}
#define PHASE_SERIAL 1 /* Input/Output */
#define PHASE_INDEX 2
#define PHASE_ENQUEUE 3
#define PHASE_FEED_WORDS 6
#define PHASE_WAIT_WORDS 4
#define PHASE_WAIT_DOCS 5
extern int phase;
extern int hku;

#define ASSERT_PHASE(asserted_phase) \
	if (phase != (asserted_phase)) { \
		fprintf(stderr, "%d:%d-%d ERROR not in phase %d %s:L%d\n", \
				thread_type, thread_id, phase, \
				asserted_phase, __FILE__, __LINE__); \
	}

#define ASSERT_THREAD(type, id) \
	if ((type) != thread_type || (id) != thread_id) { \
		fprintf(stderr, "%d:%d ERROR not %d:%d %s:L%d\n", \
				thread_type, thread_id,	 \
				(type), (id), \
				__FILE__, __LINE__); \
	} else {}
#endif
