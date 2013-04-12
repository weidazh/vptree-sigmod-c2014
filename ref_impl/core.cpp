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
using namespace std;

#define THREAD_N 4
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

struct RequestResponse {
	int tid;
	int finishing;
	DocID doc_id;
	char* doc_str; /* malloc in master thread, free after the worker received */
	DocumentResults* doc_result; /* malloc in master thread, moved to `docs' by master thread */
	pthread_mutex_t lock;
	pthread_cond_t cond;
};

struct ThreadsPool {
	int n;
	int in_flight;
	pthread_t pt[THREAD_N];
	struct RequestResponse rr[THREAD_N];

	pthread_mutex_t lock;
	pthread_cond_t cond;
	int available[THREAD_N];
};

struct ThreadsPool threadsPool;

///////////////////////////////////////////////////////////////////////////////////////////////

int CreateThread();
void KillThreads();

ErrorCode InitializeIndex(){
	threadsPool.n = 0;
	pthread_mutex_init(&threadsPool.lock, NULL);
	pthread_cond_init(&threadsPool.cond, NULL);
	while ( CreateThread() != -1);
	srand(time(NULL));

	stats.total_wait = 0;
	stats.start_serial = GetClockTimeInUS();
	stats.total_serial = 0;
	stats.start_parallel = GetClockTimeInUS();
	stats.total_parallel = 0;
	stats.start_indexing_and_query_adding = GetClockTimeInUS();
	return EC_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////////////////////

#define SHOW_STATS(total_what) ""#total_what" = %lld.%06lld\n", \
			total_what / 1000000LL, \
			total_what % 1000000LL

ErrorCode DestroyIndex(){
	stats.total_parallel += GetClockTimeInUS() - stats.start_parallel;
	fprintf(stderr, SHOW_STATS(stats.total_wait));
	fprintf(stderr, SHOW_STATS(stats.total_serial));
	fprintf(stderr, SHOW_STATS(stats.total_parallel));
	fprintf(stderr, SHOW_STATS(stats.total_indexing));
	fprintf(stderr, SHOW_STATS(stats.total_indexing_and_query_adding));

        if (THREAD_N != 1) {
		double parallel = stats.total_parallel
		                   + stats.total_indexing_and_query_adding;
		// double parallel_ut = stats.total_parallel_user_time
		//                    + stats.total_indexing_and_query_adding;
		double parallel_ut = 63e6;
		double P_estimated = (parallel / parallel_ut - 1.0) /
				     (1.0 / THREAD_N - 1.0);
		double speedup_12 = 1.0 / (1.0 - P_estimated) + (P_estimated / 12.0);
		double speedup_24 = 1.0 / (1.0 - P_estimated) + (P_estimated / 24.0);
		fprintf(stderr, "According Amdahl's law,\n");
		fprintf(stderr, "P_estimated is the portion that is parallel\n");
		fprintf(stderr, "P_estimated = %.4f\n", P_estimated);
		fprintf(stderr, "Expected speedup,time in 12 cores: %.1f, %.1f\n",
		                speedup_12, parallel_ut /1e6 / speedup_12);
		fprintf(stderr, "Expected speedup,time in 24 cores: %.1f, %.1f\n",
		                speedup_24, parallel_ut /1e6 / speedup_24);
	}
	else {
		double single_thread = 61.5e6 - stats.total_indexing;
		fprintf(stderr, "Single thread, this portion runs %.4f s\n", single_thread / 1e6);
	}
	KillThreads();
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
		fprintf(stderr, "threadsPool.in_flight > 0\n");
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
		fprintf(stderr, "threadsPool.in_flight > 0\n");
		exit(1);
	}
	VPTreeQueryRemove(query_id);
	return EC_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////////////////////

void* MTWorker(void* arg) {
	int doc_id;
	char* doc_str;
	DocID last_doc_id = INVALID_DOC_ID;
	struct RequestResponse* rr = (struct RequestResponse*)arg;
	int tid = rr->tid;
	thread_fprintf(stderr, "MTWorker[%d] starting\n", tid);
	pthread_mutex_lock(&threadsPool.lock);
	threadsPool.available[tid] = 1;
	pthread_cond_signal(&threadsPool.cond);
	pthread_mutex_unlock(&threadsPool.lock);
	pthread_mutex_lock(&rr->lock);
	while (1) {
		thread_fprintf(stderr, "MTWorker[%d] waiting\n", tid);
		pthread_cond_wait(&rr->cond, &rr->lock);
		if (rr->finishing)
			break;

		if (rr->doc_id == last_doc_id) {
			thread_fprintf(stderr, "MTWorker[%d] found old doc_id\n", tid);
			continue;
		}
		rr->doc_result = NULL;
		doc_id = rr->doc_id;
		doc_str = rr->doc_str;
		pthread_mutex_unlock(&rr->lock);

		thread_fprintf(stderr, "MTWorker[%d] found doc_id %d\n", tid, doc_id);

		vector<unsigned int> query_ids;

		VPTreeMatchDocument(doc_id, doc_str, query_ids);
		DocumentResults* doc = new DocumentResults(doc_id, query_ids);

		thread_fprintf(stderr, "MTWorker[%d] signaling rr results\n", tid);
		pthread_mutex_lock(&rr->lock);
		rr->doc_result = doc;

		thread_fprintf(stderr, "MTWorker[%d] signaling availability \n", tid);
		pthread_mutex_lock(&threadsPool.lock);
		threadsPool.available[tid] = 1;
		pthread_cond_signal(&threadsPool.cond);
		pthread_mutex_unlock(&threadsPool.lock);
	}
	pthread_mutex_unlock(&rr->lock);
	pthread_exit(NULL);
	return 0;
}

/* Only the master thread (the testdrver's thread) can call CreateThread,
 * so I do not lock the threadsPool's data structure.
 * If such behavior is changed, lock me! */
// return 1 if OK
int CreateThread() {
	int ret_val;
	int n = threadsPool.n;

	if (n >= THREAD_N)
		return -1;

	pthread_mutex_init(&threadsPool.rr[n].lock, NULL);
	pthread_cond_init(&threadsPool.rr[n].cond, NULL);
	threadsPool.rr[n].tid = n;

	ret_val = pthread_create(&threadsPool.pt[n], NULL, MTWorker, &threadsPool.rr[n]);
	if (ret_val != 0) {
		perror("Pthread create error \n");
		exit(1);
	}
	threadsPool.n ++;
	return n;
}

int FindThread(int reset_available) {
	int i;
	thread_fprintf(stderr, "MasterThread: searching workers\n");
	pthread_mutex_lock(&threadsPool.lock);
	while (1) {
		for (i = 0; i < threadsPool.n; i++) {
			if (threadsPool.available[i]) {
				thread_fprintf(stderr, "MasterThread: worker %d available\n", i);
				if (reset_available)
					threadsPool.available[i] = 0;
				pthread_mutex_unlock(&threadsPool.lock);
				return i;
			}
		}
		thread_fprintf(stderr, "MasterThread: no workers, waiting for them\n");
		pthread_cond_wait(&threadsPool.cond, &threadsPool.lock);
	}
}

// when return holding the lock
int FindThreadAndMoveBack(int reset_available) {
	int n;
	n = FindThread(reset_available);

	thread_fprintf(stderr, "MasterThread: now found %d\n", n);
	pthread_mutex_lock(&threadsPool.rr[n].lock);

	if (threadsPool.rr[n].doc_result) {
		docs.push_back(threadsPool.rr[n].doc_result);
		threadsPool.rr[n].doc_result = NULL;
		free(threadsPool.rr[n].doc_str);
	}
	return n;
}

void KillThreads() {
	int i;
	int n;
	void* ret_val;

	for(i = 0; i < THREAD_N; i++) {
		n = FindThreadAndMoveBack(1);
		threadsPool.rr[n].finishing = 1;
		pthread_cond_signal(&threadsPool.rr[n].cond);
		pthread_mutex_unlock(&threadsPool.rr[n].lock);
	}
	for(i = 0; i < THREAD_N; i++) {
		pthread_join(threadsPool.pt[i], &ret_val);
	}
}

ErrorCode MTVPTreeMatchDocument(DocID doc_id, const char* doc_str)
{
	char* cur_doc_str;
	int n;
	cur_doc_str = (char*) malloc(strlen(doc_str) + 1);
	strcpy(cur_doc_str, doc_str);

	thread_fprintf(stderr, "MasterThread: doc %d comes\n", doc_id);
	n = FindThreadAndMoveBack(1);
	threadsPool.rr[n].doc_id = doc_id;
	threadsPool.rr[n].doc_str = cur_doc_str;

	pthread_cond_signal(&threadsPool.rr[n].cond);
	pthread_mutex_unlock(&threadsPool.rr[n].lock);
	thread_fprintf(stderr, "MasterThread: signal sent to MTWorker[%d]\n", n);

	threadsPool.in_flight += 1;

	return EC_SUCCESS;
}

ErrorCode MatchDocument(DocID doc_id, const char* doc_str)
{
	if (doc_id == INVALID_DOC_ID) {
		perror("input doc_id == INVALID_DOC_ID, maybe you should use 0xFFFFFFFF as INVALID_DOC_ID instead?");
		exit(1);
	}
	if (threadsPool.in_flight == 0) {
		stats.total_serial += GetClockTimeInUS() - stats.start_serial;
	}
	return MTVPTreeMatchDocument(doc_id, doc_str);
}

///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode GetNextAvailRes(DocID* p_doc_id, unsigned int* p_num_res, QueryID** p_query_ids)
{
	int n;
	int i;
	int Q[THREAD_N];
	// Get the first undeliverd resuilt from "docs" and return it
	if (docs.size() == 0 && threadsPool.in_flight) {
		long long start = GetClockTimeInUS()
		thread_fprintf(stderr, "threadsPool.in_flight = %d\n", threadsPool.in_flight);
		n = 0;
		for (i = 0; i < threadsPool.in_flight; i++) {
			Q[i] = FindThreadAndMoveBack(1);
			n += 1;
			pthread_mutex_unlock(&threadsPool.rr[Q[i]].lock);
		}
		pthread_mutex_lock(&threadsPool.lock);
		for (i = 0; i < n; i++) {
			threadsPool.available[Q[i]] = 1;
		}
		pthread_mutex_unlock(&threadsPool.lock);
		long long end = GetClockTimeInUS();
		stats.total_wait += (end - start);
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
		thread_fprintf(stderr, "doc_id result %d\n", doc->doc_id);
		threadsPool.in_flight -= 1;
		*p_doc_id = doc->doc_id;
		*p_num_res = doc->num_res;
		*p_query_ids = doc->query_ids;
		delete doc;
		if (threadsPool.in_flight == 0)
			stats.start_serial = GetClockTimeInUS();
		return EC_SUCCESS;
	}
}

///////////////////////////////////////////////////////////////////////////////////////////////
