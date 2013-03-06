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

// Keeps all information related to an active query
struct Query
{
	QueryID query_id;
	char str[MAX_QUERY_LENGTH];
	MatchType match_type;
	unsigned int match_dist;
};

///////////////////////////////////////////////////////////////////////////////////////////////

// Keeps all query ID results associated with a dcoument
struct Document
{
	DocID doc_id;
	unsigned int num_res;
	QueryID* query_ids;
};

///////////////////////////////////////////////////////////////////////////////////////////////

// Keeps all currently active queries
vector<Query> queries;

// Keeps all currently available results that has not been returned yet
vector<Document*> docs;

///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode InitializeIndex(){return EC_SUCCESS;}

///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode DestroyIndex(){return EC_SUCCESS;}

///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode StartQuery(QueryID query_id, const char* query_str, MatchType match_type, unsigned int match_dist)
{
	Query query;
	query.query_id=query_id;
	strcpy(query.str, query_str);
	query.match_type=match_type;
	query.match_dist=match_dist;
	// Add this query to the active query set
	queries.push_back(query);

	VPTreeQueryAdd(query_id, query_str, match_type, match_dist);
	return EC_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode EndQuery(QueryID query_id)
{
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
	VPTreeQueryRemove(query_id);
	return EC_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode MatchDocument(DocID doc_id, const char* doc_str)
{
	char cur_doc_str[MAX_DOC_LENGTH];
	strcpy(cur_doc_str, doc_str);

	unsigned int i, n=queries.size();
	vector<unsigned int> query_ids;
	vector<unsigned int> hamming_query_ids;

	goto skip_to_vptree;
	// Iterate on all active queries to compare them with this new document
	for(i=0;i<n;i++)
	{
		bool matching_query=true;
		Query* quer=&queries[i];

		int iq=0;
		while(quer->str[iq] && matching_query)
		{
			while(quer->str[iq]==' ') iq++;
			if(!quer->str[iq]) break;
			char* qword=&quer->str[iq];

			int lq=iq;
			while(quer->str[iq] && quer->str[iq]!=' ') iq++;
			char qt=quer->str[iq];
			quer->str[iq]=0;
			lq=iq-lq;

			bool matching_word=false;

			int id=0;
			while(cur_doc_str[id] && !matching_word)
			{
				while(cur_doc_str[id]==' ') id++;
				if(!cur_doc_str[id]) break;
				char* dword=&cur_doc_str[id];

				int ld=id;
				while(cur_doc_str[id] && cur_doc_str[id]!=' ') id++;
				char dt=cur_doc_str[id];
				cur_doc_str[id]=0;

				ld=id-ld;

				if(quer->match_type==MT_EXACT_MATCH)
				{
					if(strcmp(qword, dword)==0) matching_word=true;
				}
				else if(quer->match_type==MT_HAMMING_DIST)
				{
					unsigned int num_mismatches=HammingDistance(qword, lq, dword, ld);
					if(num_mismatches<=quer->match_dist) matching_word=true;
				}
				else if(quer->match_type==MT_EDIT_DIST)
				{
					unsigned int edit_dist=EditDistance(qword, lq, dword, ld);
					if(edit_dist<=quer->match_dist) matching_word=true;
				}

				cur_doc_str[id]=dt;
			}

			quer->str[iq]=qt;

			if(!matching_word)
			{
				// This query has a word that does not match any word in the document
				matching_query=false;
			}
		}

		if(matching_query)
		{
			// This query matches the document
			query_ids.push_back(quer->query_id);
			if (quer->match_type == MT_HAMMING_DIST) {
			    hamming_query_ids.push_back(quer->query_id);
			}
		}
	}

skip_to_vptree:
	VPTreeMatchDocument(doc_id, doc_str, query_ids);

	Document* doc = new Document();
	doc->doc_id=doc_id;
	doc->num_res=query_ids.size();
	doc->query_ids=0;
	if(doc->num_res) doc->query_ids=(unsigned int*)malloc(doc->num_res*sizeof(unsigned int));
	for(i=0;i<doc->num_res;i++) doc->query_ids[i]=query_ids[i];
	// Add this result to the set of undelivered results
	docs.push_back(doc);

	return EC_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode GetNextAvailRes(DocID* p_doc_id, unsigned int* p_num_res, QueryID** p_query_ids)
{
	Document* doc = docs[0];
	// Get the first undeliverd resuilt from "docs" and return it
	*p_doc_id=0; *p_num_res=0; *p_query_ids=0;
	if(docs.size()==0) return EC_NO_AVAIL_RES;
	*p_doc_id=doc->doc_id; *p_num_res=doc->num_res; *p_query_ids=doc->query_ids;
	docs.erase(docs.begin());
	delete doc;
	return EC_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////////////////////
