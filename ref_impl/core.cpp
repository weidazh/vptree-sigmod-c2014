/*
 * core.cpp version 1.0
 * Copyright (c) 2013 KAUST - InfoCloud Group (All Rights Reserved)
 * Author: Amin Allam
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "../include/core.h"
#include "vptree_match.h"
#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <vector>
#include <ctime>
#include <pthread.h>
#include "common.h"
#include "utils.h"
using namespace std;

// Threads to create
int doc_worker_n = DOC_WORKER_N;
__thread int thread_id;
__thread int thread_type;
__thread int thread_sid;
__thread long long thread_total_resultmerging;
int phase;
int n_phases = 0;
int hku = 0;
FILE* logf = stdout;
// Then `Threads create' is threadsPool.n

#define INVALID_DOC_ID 0
#define thread_fprintf(...)
// #define thread_fprintf fprintf
////////////////
struct stats stats;
///////////////////////////////////////////////////////////////////////////////////////////////

// Computes edit distance between a null-terminated string "a" with length "na"
//  and a null-terminated string "b" with length "nb" 
static inline int min2(int v, int b) {
	if (v < b)
		return v;
	else
		return b;
}
#define likely(x) __builtin_expect((x),1)
#define unlikely(x) __builtin_expect((x),0)
static inline int EditDistanceCore(const char* a, int na, const char* b, int nb) {
	int T[2][MAX_WORD_LENGTH]; // gprof: static or not, no difference
	int cur = 0;
	int best;
	// loop unrolling
	{
		best = T[cur][0] = (a[0] != b[0]);
		for (int ib = 1; ib < nb; ib ++) { // do not use v, v1, v2, 0.5s faster
			best = min2(best + 1,
				ib + unlikely(a[0] != b[ib]));
			T[cur][ib] = best;
		}
	}
	for (int ia = 1; ia < na; ia++) {
		cur = !cur;
		T[cur][0] = min2(ia + (a[ia] != b[0]), T[!cur][0] + 1);
		best = T[cur][0]; // use best, 0.8s faster
		for (int ib = 1; ib < nb; ib ++) { // do not use v, v1, v2, 0.5s faster
			if (unlikely(T[!cur][ib] < best))
				best = T[!cur][ib];
			best = min2(best + 1,
				T[!cur][ib - 1] + unlikely(a[ia] != b[ib]));
			// likely a = b? why? but test show me likely is 0.3s faster
			T[cur][ib] = best;
		}
	}
	return best;
}
int EditDistance(const char* a, int na, const char* b, int nb) {
	if (na > nb) {
		return EditDistanceCore(b, nb, a, na);
	}
	else {
		return EditDistanceCore(a, na, b, nb);
	}
}
///////////////////////////////////////////////////////////////////////////////////////////////

// Computes Hamming distance between a null-terminated string "a" with length "na"
//  and a null-terminated string "b" with length "nb" 
unsigned int HammingDistance(const char* a, int na, const char* b, int nb)
{
	int j, oo=0x7FFFFFFF;
	if(na!=nb) return oo;
	
	unsigned int num_mismatches=0;
	for(j=0;j<na;j++) if(a[j]!=b[j]) num_mismatches++;
	
	return num_mismatches;
}

///////////////////////////////////////////////////////////////////////////////////////////////

#if 0
// Keeps all information related to an active query
struct Query
{
	QueryID query_id;
	char str[MAX_QUERY_LENGTH];
	MatchType match_type;
	unsigned int match_dist;
};
#endif
///////////////////////////////////////////////////////////////////////////////////////////////

// Keeps all query ID results associated with a dcoument
struct DocumentResults
{
	DocID doc_id;
	unsigned int num_res;
	QueryID* query_ids;

	DocumentResults(DocID doc_id, std::vector<QueryID> query_ids):
		doc_id(doc_id),
		num_res(query_ids.size()) {
		/* Only this case I use malloc, because the driver will free it */
		QueryID* p = (QueryID*) malloc(num_res * sizeof(QueryID));
		this->query_ids = p;
		for (std::vector<QueryID>::iterator i = query_ids.begin();
			i != query_ids.end();
			i++, p++) {
			*p = *i;
		}
	}
};

///////////////////////////////////////////////////////////////////////////////////////////////

#if 0
// Keeps all currently active queries
vector<Query> queries;
#endif

// Keeps all currently available results that has not been returned yet
pthread_mutex_t docs_lock = PTHREAD_MUTEX_INITIALIZER;
vector<DocumentResults*> docs;
#define MAX_BATCH_DOC (48 * 4)
struct RequestResponse {
	// int tid;
	// int finishing;
	DocID doc_id;
	const char* doc_str; /* malloc in master thread, free after the worker received */
	DocumentResults* doc_result; /* malloc in master thread, moved to `docs' by master thread */
	// pthread_mutex_t lock;
	// pthread_cond_t cond;
};

struct ThreadsPool {
	int n;
	int in_flight;
	pthread_t pt[DOC_WORKER_N];
	struct RequestResponse queue[MAX_BATCH_DOC];
	int head;
	int tail;
	int result_head;
	int finishing;

	pthread_mutex_t lock;
	pthread_cond_t cond;
	pthread_mutex_t resplock;
	pthread_cond_t respcond;
	// int available[DOC_WORKER_N];
};

struct ThreadsPool threadsPool;

///////////////////////////////////////////////////////////////////////////////////////////////

int CreateThread();
void KillThreads();

void setPhase(int phase_to_be) {
	static const char* PROG = " 123456789abcdefghijklmnop";
	static int progbar = 0;
	if (phase == 0 && hku) {
		progbar = 1;
		fprintf(logf, "\n\n.");
	}
	phase = phase_to_be;
	if (phase_to_be == PHASE_SERIAL)
		n_phases += 1;
	if (progbar) {
		if (n_phases % 8 == 0 && phase_to_be == PHASE_SERIAL) {
			fprintf(logf, "\b..");
		}
		fprintf(logf, "\b%c", PROG[phase_to_be]);
	}
}

static void CreateFeederWaiterIndexerThreads();
ErrorCode InitializeIndex(){
	setThread(MASTER_THREAD, 0);
	if (strcmp(getenv("HOSTNAME"), "sg010") == 0) {
		hku = 1;
		logf = stderr;
	}


	char* env_doc_worker_n;
	if ((env_doc_worker_n = getenv("DOC_WORKER_N")) != NULL) {
		doc_worker_n = atoi(env_doc_worker_n);
	}
	if (doc_worker_n > DOC_WORKER_N) {
		fprintf(logf, "doc_worker_n(%d) > DOC_WORKER_N\n", doc_worker_n);
		exit(1);
	}
	threadsPool.n = 0;
	pthread_mutex_init(&threadsPool.lock, NULL);
	pthread_cond_init(&threadsPool.cond, NULL);
	pthread_mutex_init(&threadsPool.resplock, NULL);
	pthread_cond_init(&threadsPool.respcond, NULL);

	vptree_system_init();
	fprintf(logf, "doc_worker_n = %d\n", doc_worker_n);

	setPhase(PHASE_SERIAL);
	while ( CreateThread() != -1);
	srand(time(NULL));

	stats.total_wait = 0;
	stats.total_words_wait = 0;
	stats.total_docs_wait = 0;
	stats.start_serial = GetClockTimeInUS();
	stats.total_serial = 0;
	stats.start_indexing_and_query_adding = GetClockTimeInUS();

	CreateFeederWaiterIndexerThreads();
	return EC_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////////////////////

#define SHOW_STATS(sym, total_what) ""sym" = %lld.%06lld\n", \
			total_what / 1000000LL, \
			total_what % 1000000LL

ErrorCode DestroyIndex(){
	KillThreads();
	vptree_system_destroy();

	fprintf(logf, "n_phases %d\n", n_phases);
	fprintf(logf, "\n");
	fprintf(logf, SHOW_STATS("master index", stats.total_master_indexing));
	fprintf(logf, SHOW_STATS("  indexing", stats.total_indexing));
	fprintf(logf, SHOW_STATS("  indexing2", stats.total_indexing_and_query_adding));
	fprintf(logf, SHOW_STATS("enqueue", stats.total_enqueuing));
	fprintf(logf, SHOW_STATS("wait", stats.total_wait));
	fprintf(logf, SHOW_STATS("  feed", stats.total_feed));
	fprintf(logf, SHOW_STATS("  words", stats.total_words_wait));
	fprintf(logf, "    (feed + words_wait) %lld.%06lld\n",
		(stats.total_words_wait + stats.total_feed)/1000000LL,
		(stats.total_words_wait + stats.total_feed)%1000000LL);
	fprintf(logf, SHOW_STATS("  docs ", stats.total_docs_wait));
	fprintf(logf, SHOW_STATS("    (last half)", stats.total_docs_wait_small));
	fprintf(logf, SHOW_STATS("    merge", stats.total_resultmerging));
	fprintf(logf, "    merge / docs_wait = %.2f (expect 12)\n",
		(double)stats.total_resultmerging / stats.total_docs_wait );
	fprintf(logf, SHOW_STATS("serial", stats.total_serial));
	fprintf(logf, "\n");

	return EC_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode StartQuery(QueryID query_id, const char* query_str, MatchType match_type, unsigned int match_dist)
{
#if 0
	Query query;
	query.query_id=query_id;
	strcpy(query.str, query_str);
	query.match_type=match_type;
	query.match_dist=match_dist;
	// Add this query to the active query set
	queries.push_back(query);
#endif
	if (threadsPool.in_flight > 0) {
		fprintf(logf, "threadsPool.in_flight > 0\n");
		exit(1);
	}

	VPTreeQueryAdd(query_id, query_str, match_type, match_dist);
	return EC_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode EndQuery(QueryID query_id)
{
#if 0
	// Remove this query from the active query set
	unsigned int i, n=queries.size();
	for(i=0;i<n;i++)
	{
		if(queries[i].query_id==query_id)
		{
			queries.erase(queries.begin()+i);
			break;
		}
	}
#endif
	if (threadsPool.in_flight > 0) {
		fprintf(logf, "threadsPool.in_flight > 0\n");
		exit(1);
	}
	VPTreeQueryRemove(query_id);
	return EC_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////////////////////

struct MTWorkerArg {
	int tid;
	MTWorkerArg(int tid): tid(tid) {}
};
void* MTWorker(void* arg) {
	struct MTWorkerArg* MTWorkerArg = (struct MTWorkerArg*) arg;
	int tid = MTWorkerArg->tid;
	struct ThreadsPool* rr = &threadsPool;
	DELETE(MTWorkerArg);

	setThread(DOC_WORKER_THREAD, tid);
	StickToCores(DOC_WORKER_THREAD, thread_id, doc_worker_n);

	thread_fprintf(logf, "MTWorker[%d] starting\n", tid);
	vptree_doc_worker_init();

	// pthread_mutex_lock(&threadsPool.lock);
	// threadsPool.available[tid] = 1;
	// pthread_cond_signal(&threadsPool.cond);
	// pthread_mutex_unlock(&threadsPool.lock);

	pthread_mutex_lock(&rr->lock);
	while (! rr->finishing) {
		thread_fprintf(logf, "MTWorker[%d] waiting\n", tid);
		while(rr->head == rr->tail && !rr->finishing) {
			pthread_cond_wait(&rr->cond, &rr->lock);
		}
		if (rr->finishing)
			break;

		struct RequestResponse* doc = &rr->queue[rr->head];
		rr->head = (rr->head + 1) % MAX_BATCH_DOC;
		pthread_mutex_unlock(&rr->lock);

		doc->doc_result = NULL;
		DocID doc_id = doc->doc_id;
		const char* doc_str = doc->doc_str;

		thread_fprintf(logf, "MTWorker[%d] found doc_id %d\n", tid, doc_id);

		vector<unsigned int> query_ids;

		VPTreeMatchDocument(doc_id, doc_str, query_ids);
		doc->doc_result = new DocumentResults(doc_id, query_ids);

#if 0
		thread_fprintf(logf, "MTWorker[%d] signaling rr results\n", tid);
		pthread_mutex_lock(&rr->lock);
		rr->doc_result = doc;

		thread_fprintf(logf, "MTWorker[%d] signaling availability \n", tid);
		pthread_mutex_lock(&threadsPool.lock);
		threadsPool.available[tid] = 1;
		pthread_cond_signal(&threadsPool.cond);
		pthread_mutex_unlock(&threadsPool.lock);
#endif
		pthread_mutex_lock(&rr->lock);
		if (rr->head == rr->tail) {
			pthread_mutex_lock(&rr->resplock);
			pthread_cond_signal(&rr->respcond);
			pthread_mutex_unlock(&rr->resplock);
		}
	}
	pthread_mutex_unlock(&rr->lock);
	pthread_mutex_lock(&threadsPool.lock);
	stats.total_resultmerging += thread_total_resultmerging;
	pthread_mutex_unlock(&threadsPool.lock);
	vptree_doc_worker_destroy();
	pthread_exit(NULL);
	return 0;
}

/* Only the master thread (the testdrver's thread) can call CreateThread,
 * so I do not lock the threadsPool's data structure.
 * If such behavior is changed, lock me! */
// return 1 if OK
int CreateThread() {
	int tid = threadsPool.n;

	if (tid >= doc_worker_n)
		return -1;
#if 0
	pthread_mutex_init(&threadsPool.rr[n].lock, NULL);
	pthread_cond_init(&threadsPool.rr[n].cond, NULL);
	threadsPool.rr[n].tid = n;
#endif

	int ret_val = pthread_create(&threadsPool.pt[tid], NULL, MTWorker, new MTWorkerArg(tid));
	if (ret_val != 0) {
		perror("Pthread create error \n");
		exit(1);
	}
	threadsPool.n ++;
	return tid;
}

#if 0
int FindThread(int reset_available) {
	int i;
	thread_fprintf(logf, "MasterThread: searching workers\n");
	pthread_mutex_lock(&threadsPool.lock);
	while (1) {
		for (i = 0; i < threadsPool.n; i++) {
			if (threadsPool.available[i]) {
				thread_fprintf(logf, "MasterThread: worker %d available\n", i);
				if (reset_available)
					threadsPool.available[i] = 0;
				pthread_mutex_unlock(&threadsPool.lock);
				return i;
			}
		}
		thread_fprintf(logf, "MasterThread: no workers, waiting for them\n");
		pthread_cond_wait(&threadsPool.cond, &threadsPool.lock);
	}
}

// when return holding the lock
int FindThreadAndMoveBack(int reset_available) {
	int n;
	n = FindThread(reset_available);

	thread_fprintf(logf, "MasterThread: now found %d\n", n);
	pthread_mutex_lock(&threadsPool.rr[n].lock);

	if (threadsPool.rr[n].doc_result) {
		docs.push_back(threadsPool.rr[n].doc_result);
		threadsPool.rr[n].doc_result = NULL;
		FREE(threadsPool.rr[n].doc_str);
	}
	return n;
}
#endif

void KillThreads() {
	pthread_mutex_lock(&threadsPool.lock);
	threadsPool.finishing = 1;
	pthread_cond_broadcast(&threadsPool.cond);
	pthread_mutex_unlock(&threadsPool.lock);

	for(int tid = 0; tid < threadsPool.n; tid++) {
		pthread_join(threadsPool.pt[tid], NULL);
	}
}

ErrorCode MTVPTreeMatchDocument(DocID doc_id, const char* doc_str)
{
	char* cur_doc_str = (char*) MALLOC(strlen(doc_str) + 1);
	strcpy(cur_doc_str, doc_str); // FIXME: Who is freeing it?

	// enqueue
	VPTreeMasterMatchDocument(doc_id, doc_str);

	thread_fprintf(logf, "MasterThread: doc %d comes\n", doc_id);
	pthread_mutex_lock(&threadsPool.lock);
	if (((threadsPool.tail + 1) % MAX_BATCH_DOC == threadsPool.result_head) ||
	    ((threadsPool.tail + 1) % MAX_BATCH_DOC == threadsPool.head)) {
		pthread_mutex_unlock(&threadsPool.lock);
		fprintf(logf, "MAX_BATCH_DOC is not large enough\n");
		exit(1);
	}
	threadsPool.queue[threadsPool.tail].doc_id = doc_id;
	threadsPool.queue[threadsPool.tail].doc_str = cur_doc_str;
	threadsPool.queue[threadsPool.tail].doc_result = NULL;
	threadsPool.tail = (threadsPool.tail + 1) % MAX_BATCH_DOC;
	/* do not send signal until wait */
	// pthread_cond_signal(&threadsPool.cond, &threadsPool.lock);
	pthread_mutex_unlock(&threadsPool.lock);
#if 0
	// FIXME!!!! I forgot to lock this before ?
	n = FindThreadAndMoveBack(1);
	threadsPool.rr[n].doc_id = doc_id;
	threadsPool.rr[n].doc_str = cur_doc_str;

	pthread_cond_signal(&threadsPool.rr[n].cond);
	pthread_mutex_unlock(&threadsPool.rr[n].lock);
	thread_fprintf(logf, "MasterThread: signal sent to MTWorker[%d]\n", n);
#endif

	ASSERT_THREAD(MASTER_THREAD, 0);
	threadsPool.in_flight += 1;

	return EC_SUCCESS;
}

pthread_barrier_t indexer_start_barr;
pthread_barrier_t indexer_end_barr;
ErrorCode MatchDocument(DocID doc_id, const char* doc_str)
{
	if (doc_id == INVALID_DOC_ID) {
		perror("input doc_id == INVALID_DOC_ID, maybe you should use 0xFFFFFFFF as INVALID_DOC_ID instead?");
		exit(1);
	}
	if (threadsPool.in_flight == 0) {
		stats.total_serial += GetClockTimeInUS() - stats.start_serial;
		long long start = GetClockTimeInUS();

		setPhase(PHASE_INDEX);
		BuildIndexPre();
		pthread_barrier_wait(&indexer_start_barr);
		// wait
		pthread_barrier_wait(&indexer_end_barr);
		BuildIndexPost();

		setPhase(PHASE_ENQUEUE);
		stats.total_master_indexing += GetClockTimeInUS() - start;
		stats.start_enqueuing = GetClockTimeInUS();
	}
	return MTVPTreeMatchDocument(doc_id, doc_str);
}

void WaitDocumentResults() {
	pthread_mutex_lock(&threadsPool.resplock);
	pthread_cond_broadcast(&threadsPool.cond);
	// fprintf(logf, "threadsPool.in_flight = %d\n", threadsPool.in_flight);
	long long start_small_wait = -1;
	while(threadsPool.in_flight) {
		struct RequestResponse* resp = &threadsPool.queue[threadsPool.result_head];
		while (resp->doc_result == NULL) {
			pthread_cond_wait(&threadsPool.respcond, &threadsPool.resplock);
		}

		docs.push_back(resp->doc_result);
		threadsPool.result_head = (threadsPool.result_head + 1) % MAX_BATCH_DOC;
		threadsPool.in_flight -= 1;
		if (threadsPool.in_flight * 2 == doc_worker_n) {
			start_small_wait = GetClockTimeInUS();
		}
	}
	pthread_mutex_unlock(&threadsPool.resplock);

	if (start_small_wait >= 0) {
		stats.total_docs_wait_small = GetClockTimeInUS() - start_small_wait;
	}
}

int barriers_init_ed = 0;
pthread_barrier_t feeder_start_barr;
pthread_barrier_t feeder_end_barr;
pthread_barrier_t waiter_start_barr;
pthread_barrier_t waiter_end_barr;

struct WordFeederWaiterArg {
	int waiter_id;
	WordFeederWaiterArg(int id): waiter_id(id) {}
};
void* feeder_waiter_thread(void* arg) {
	int id = ((WordFeederWaiterArg*)arg)->waiter_id;
	// setThread(WORD_FEEDER_THREAD, id);
	setThread(WORD_WAITER_THREAD, id);
	StickToCores(WORD_WAITER_THREAD, id, word_feeder_n);
	while (true) {
		pthread_barrier_wait(&feeder_start_barr);
		VPTreeWordFeeder(id);
		pthread_barrier_wait(&feeder_end_barr);

		pthread_barrier_wait(&waiter_start_barr);
		WaitWordResults(id);
		pthread_barrier_wait(&waiter_end_barr);
	}
	pthread_exit(NULL);
	return NULL;
}

struct IndexerArg {
	int indexer_id;
	IndexerArg(int id): indexer_id(id) {}
};
void* indexer_thread(void* arg) {
	int id = ((IndexerArg*)arg)->indexer_id;
	setThread(INDEXER_THREAD, id);
	StickToCores(INDEXER_THREAD, id, indexer_n);
	while (true) {
		pthread_barrier_wait(&indexer_start_barr);
		BuildIndexThread();
		pthread_barrier_wait(&indexer_end_barr);
	}
	return 0;
}

static void CreateFeederWaiterIndexerThreads() {
	if (barriers_init_ed) {
		return;
	}
	pthread_barrier_init(&feeder_start_barr, NULL, 1 + word_feeder_n);
	pthread_barrier_init(&feeder_end_barr, NULL, 1 + word_feeder_n);
	pthread_barrier_init(&waiter_start_barr, NULL, 1 + word_waiter_n);
	pthread_barrier_init(&waiter_end_barr, NULL, 1 + word_waiter_n);
	pthread_t waiter_threads[word_waiter_n];
	for (int i = 0; i < word_waiter_n; i++) {
		pthread_create(&waiter_threads[i], NULL, feeder_waiter_thread, NEW(WordFeederWaiterArg, i));
	}

	pthread_barrier_init(&indexer_start_barr, NULL, 1 + indexer_n);
	pthread_barrier_init(&indexer_end_barr, NULL, 1 + indexer_n);
	pthread_t indexer_threads[indexer_n];
	for (int i = 0; i < indexer_n; i++) {
		pthread_create(&indexer_threads[i], NULL, indexer_thread, NEW(IndexerArg, i));
	}

	barriers_init_ed = 1;
}

void WaitResults() {
	long long start = GetClockTimeInUS();

	setPhase(PHASE_FEED_WORDS);
	pthread_barrier_wait(&feeder_start_barr);
	// feed
	pthread_barrier_wait(&feeder_end_barr);

	long long after_feed = GetClockTimeInUS();

	setPhase(PHASE_WAIT_WORDS);
	pthread_barrier_wait(&waiter_start_barr);
	// wait word to finish
	pthread_barrier_wait(&waiter_end_barr);
	MasterMergeCache();
	long long mid = GetClockTimeInUS();

	setPhase(PHASE_WAIT_DOCS);
	WaitDocumentResults();

	setPhase(PHASE_SERIAL);

	long long end = GetClockTimeInUS();
	stats.total_wait += (end - start);
	stats.total_feed += (after_feed - start);
	stats.total_words_wait += (mid - after_feed);
	stats.total_docs_wait += (end - mid);
}

///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode GetNextAvailRes(DocID* p_doc_id, unsigned int* p_num_res, QueryID** p_query_ids)
{
	// Get the first undeliverd resuilt from "docs" and return it
	if (docs.size() == 0) {
		stats.total_enqueuing += GetClockTimeInUS() - stats.start_enqueuing;
		WaitResults();
		// fprintf(logf, ".");
	}
	if (docs.size() == 0) {
		*p_doc_id = 0;
		*p_num_res = 0;
		*p_query_ids = 0;
		return EC_NO_AVAIL_RES;
	}
	else {
		DocumentResults* doc = docs[0];
		docs.erase(docs.begin());
		thread_fprintf(logf, "doc_id result %d\n", doc->doc_id);
		*p_doc_id = doc->doc_id;
		*p_num_res = doc->num_res;
		*p_query_ids = doc->query_ids;
		// delete doc;
		DELETE(doc);
		if (threadsPool.in_flight == 0)
			stats.start_serial = GetClockTimeInUS();
		return EC_SUCCESS;
	}
}

///////////////////////////////////////////////////////////////////////////////////////////////
