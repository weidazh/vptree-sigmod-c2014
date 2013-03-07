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
using namespace std;

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
	static int T[2][MAX_WORD_LENGTH]; // gprof: static or not, no difference
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
vector<DocumentResults*> docs;

///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode InitializeIndex(){
	srand(time(NULL));
	return EC_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode DestroyIndex(){return EC_SUCCESS;}

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
	VPTreeQueryRemove(query_id);
	return EC_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode MatchDocument(DocID doc_id, const char* doc_str)
{
	char cur_doc_str[MAX_DOC_LENGTH];
	strcpy(cur_doc_str, doc_str);

	vector<unsigned int> query_ids;

	VPTreeMatchDocument(doc_id, cur_doc_str, query_ids);

	DocumentResults* doc = new DocumentResults(doc_id, query_ids);
	docs.push_back(doc);

	return EC_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode GetNextAvailRes(DocID* p_doc_id, unsigned int* p_num_res, QueryID** p_query_ids)
{
	// Get the first undeliverd resuilt from "docs" and return it
	if (docs.size() == 0) {
		*p_doc_id = 0;
		*p_num_res = 0;
		*p_query_ids = 0;
		return EC_NO_AVAIL_RES;
	}
	else {
		DocumentResults* doc = docs[0];
		docs.erase(docs.begin());
		*p_doc_id = doc->doc_id;
		*p_num_res = doc->num_res;
		*p_query_ids = doc->query_ids;
		delete doc;
		return EC_SUCCESS;
	}
}

///////////////////////////////////////////////////////////////////////////////////////////////
