#include "vptree_match.h"
#include "vptree.h"
#include <cstdio>
#include <string>
#include <cstring>
#include <exception>
#include <algorithm>
#include <vector>
#include <set>
#include <map>
#include "common.h"
#include "utils.h"

#define thread_fprintf(...)
// #define thread_fprintf fprintf
#define NON_NULL(a) ((*(a)) && ((*(a)) != ' '))

unsigned int HammingDistance(const char* a, int na, const char* b, int nb);
int EditDistance(const char* a, int na, const char* b, int nb);

#define TAU 4

/* Iterator to pointer */
#define I2P(x) (&(*(x)))

#define ENABLE_RESULT_CACHE 1
#define ENABLE_THREAD_RESULT_CACHE 1
#define ENABLE_WAITER_LOCAL_CACHE 1
#define ENABLE_GLOBAL_RESULT_CACHE 1

#define ENABLE_MULTI_EDITVPTREE 1
#define ENABLE_SEPARATE_EDIT123 1

typedef int WordIDType;
#define MAX_INTEGER 0x7fffffff
static __thread long long perf_counter_hamming = 0;
static __thread long long perf_counter_edit = 0;
pthread_mutex_t global_counter_lock = PTHREAD_MUTEX_INITIALIZER;
long long global_perf_counter_hamming = 0;
long long global_perf_counter_edit = 0;
long long global_perf_counter_index_hamming = 0;
long long global_perf_counter_index_edit = 0;
// must be int

#define ENABLE_MYSTRING 1
#if ENABLE_MYSTRING
struct mystring_alt {
	char x[MAX_WORD_LENGTH + 1];
	mystring_alt(const char* x) {
		memcpy(this->x, x, MAX_WORD_LENGTH);
		this->x[MAX_WORD_LENGTH] = 0;
	}
};
struct mystring {
	char x[MAX_WORD_LENGTH]; // 31
	char len;

	mystring():len(0) {x[0] = 0;}

	mystring(const char* w) {
		const char* p = w;
		while(NON_NULL(p))
			p++;
		int len = p - w;
		memcpy(x, w, len);
		x[len] = 0;
		this->len = len;
	}


	const char* c_str() const{
		if (len == 31)
			return (NEW(mystring_alt, x))->x;
		return x;
	}
	int length() const{
		return len;
	}
	bool operator < (const mystring& other) const{
		return memcmp(x, other.x, len + 1) < 0;
	}
};
#else
typedef std::string mystring;
#endif

int hamming(const mystring& a, const mystring& b) {
	unsigned int oo = 0x7FFFFFFF;
	unsigned int dist = HammingDistance(a.c_str(), a.length(), b.c_str(), b.length());
	perf_counter_hamming += 1;
	if (dist == oo) {
		return oo - TAU;
	}
	return dist;
}

int edit(const mystring& a, const mystring& b) {
	unsigned int oo = 0x7FFFFFFF;
	unsigned int dist = EditDistance(a.c_str(), a.length(), b.c_str(), b.length());
	perf_counter_edit += 1;
	if (dist == oo) {
		return oo - TAU;
	}
	return dist;
}

class Query {
private:
	char query_str[MAX_QUERY_LENGTH];
public:
	WordIDType word_ids[MAX_QUERY_WORDS];
	QueryID query_id;
	MatchType match_type;
	unsigned int match_dist;

	Query(QueryID query_id, const char* query_str, MatchType match_type, unsigned int match_dist) {
		this->query_id = query_id;
		strcpy(this->query_str, query_str);
		this->match_type = match_type;
		this->match_dist = match_dist;
		for (int i = 0; i < MAX_QUERY_WORDS; i++)
			word_ids[i] = -1;
	}

	const char* getQueryStr() {
		return query_str;
	}
};

// pthread_mutex_t max_word_id_lock = PTHREAD_MUTEX_INITIALIZER;
// static WordIDType max_word_id = 0;

// pthread_rwlock_t wordLock = PTHREAD_RWLOCK_INITIALIZER;
__thread WordIDType thread_max_word_id = 0;
class Word {
	mystring word;
	// int hamming_queries[TAU];
	int hamming_queries;
	int edit_queries[TAU];
	std::set<QueryID> first_word_queries;
	WordIDType word_id;

public:
	Word(mystring word)
		: word(word), hamming_queries(0), first_word_queries() {
		memset(edit_queries, 0, sizeof(edit_queries));

		// pthread_mutex_lock(&max_word_id_lock);
#if 0
		word_id = thread_max_word_id * DOC_WORKER_N + thread_id;
#else
		ASSERT_THREAD(MASTER_THREAD, 0);
		word_id = thread_max_word_id;
#endif
		if (word_id >= MAX_INTEGER) {
			fprintf(logf, "Yes, you should not use int as WordIDType\n");
		}
		thread_max_word_id += 1;
		// pthread_mutex_unlock(&max_word_id_lock);
	}
	void push_query(QueryID q, bool first, MatchType match_type, unsigned int match_dist) {
		// pthread_rwlock_wrlock(&wordLock);
		ASSERT_THREAD(MASTER_THREAD, 0);
		if (match_type == MT_HAMMING_DIST || match_type == MT_EXACT_MATCH)
			this->hamming_queries += 1;
			// this->hamming_queries[match_dist] += 1;
		if (match_type == MT_EDIT_DIST)
			this->edit_queries[match_dist] += 1;
		if (first)
			this->first_word_queries.insert(q);
		// pthread_rwlock_unlock(&wordLock);
	}
	void remove_query(QueryID q, MatchType match_type, unsigned int match_dist) {
		// pthread_rwlock_wrlock(&wordLock);
		ASSERT_THREAD(MASTER_THREAD, 0);
		if (match_type == MT_HAMMING_DIST || match_type == MT_EXACT_MATCH)
			this->hamming_queries -= 1;
		if (match_type == MT_EDIT_DIST)
			this->edit_queries[match_dist] -= 1;
		// pthread_rwlock_unlock(&wordLock);
	}
	void remove_first_word_query(QueryID q) {
		// pthread_rwlock_wrlock(&wordLock);
		ASSERT_THREAD(MASTER_THREAD, 0);
		this->first_word_queries.erase(q);
		// pthread_rwlock_unlock(&wordLock);
	}
	std::set<QueryID>::iterator begin() const{
		return first_word_queries.begin();
	}
	std::set<QueryID>::iterator end() const{
		return first_word_queries.end();
	}
	WordIDType id() const {
		return word_id;
	}
	bool empty() const {
		if (hasHamming())
			return false;
		return !hasEdit();
	}
	bool hasHamming() const {
		return !! this->hamming_queries;
	}
	bool hasEdit() const {
		for (int i = 1; i < TAU; i++) {
			if (this->edit_queries[i])
				return true;
		}
		return false;
	}
	bool hasEdit(int i) const {
		return this->edit_queries[i];
	}
};

typedef std::map<mystring, Word*> WordMap;
typedef std::map<WordIDType, Word*> WordMapByID;
// pthread_rwlock_t wordMapLock = PTHREAD_RWLOCK_INITIALIZER;
WordMap wordMap;
WordMapByID wordMapByID;
// std::set<mystring> wordSet;
typedef VpTree<mystring, int, hamming> HammingVpTree;
typedef VpTree<mystring, int, edit> EditVpTree;
HammingVpTree* hamming_vptree;
EditVpTree* edit_vptree[TAU][MAX_WORD_LENGTH + 1];

typedef std::map<QueryID, Query*> QueryMap;
// pthread_rwlock_t queryMapLock = PTHREAD_RWLOCK_INITIALIZER;
QueryMap queryMap;


class ResultSet {
public:
	WordIDType* results_hamming[TAU];
	WordIDType* results_edit[TAU];
	~ResultSet() {
		for(int i = 0; i < TAU; i++)
			FREE(results_hamming[i]);
		for(int i = 0; i < TAU; i++)
			FREE(results_edit[i]);
	}
};

#if ENABLE_RESULT_CACHE
typedef std::map<mystring, ResultSet*> ResultCache;
#if ENABLE_GLOBAL_RESULT_CACHE
// pthread_rwlock_t resultCacheLock = PTHREAD_RWLOCK_INITIALIZER;
ResultCache resultCache;
#endif
#if ENABLE_THREAD_RESULT_CACHE
const int thread_result_cache_n = WORD_FEEDER_N;
ResultCache __threadResultCache[WORD_FEEDER_N];
__thread ResultCache* threadResultCache = NULL;
#endif
#endif

static const char* next_word_in_query(const char* query_str) {
	while (NON_NULL(query_str))
		query_str ++;
	while (*query_str == ' ')
		query_str ++;
	return query_str;
}

static mystring word_to_string(const char* word) {
#if ENABLE_MYSTRING
	return mystring(word);
#else
	char w[MAX_WORD_LENGTH + 1];
	char* p = w;
	while(NON_NULL(word)) {
		*p = *word;
		word++;
		p++;
	}
	*p = 0;
	return mystring(w);
#endif
}

#define ITERATE_QUERY_WORDS(key, begin) for (const char* (key) = (begin); *(key); (key) = next_word_in_query((key)))

// int old_perf_hamming;
// int old_perf_edit;
// return whether or not a new tree is created
static int new_vptrees_unless_exists() {
	if (thread_type != MASTER_THREAD || thread_id != 0)
		return 0;

	std::vector<mystring> hammingWordList;
	std::vector<mystring> editWordList[TAU];
	if (! hamming_vptree) {
		long long start = GetClockTimeInUS();
		long long old_perf_hamming = perf_counter_hamming;
		long long old_perf_edit = perf_counter_edit;
		hamming_vptree = NEW(HammingVpTree);
#if ENABLE_SEPARATE_EDIT123
		for (int ed = 1; ed < TAU; ed++) {
#else
		for (int ed = 0; ed < 1; ed++) {
#endif
			for (int i = 0; i <= MAX_WORD_LENGTH; i++) {
				edit_vptree[ed][i] = NEW(EditVpTree);
			}
		}
		// pthread_rwlock_rdlock(&wordMapLock);
		ASSERT_PHASE(PHASE_INDEX);
		for(std::map<mystring, Word*>::iterator i = wordMap.begin();
			i != wordMap.end(); i++) {

			Word* w = I2P(i->second);

			if (w->hasHamming())
				hammingWordList.push_back(i->first);
#if ENABLE_SEPARATE_EDIT123
#if 0
			for (int ed = 1; ed < TAU; ed++) {
				if (w->hasEdit(ed))
					editWordList[ed].push_back(i->first);
			}
#else
			for (int ed = TAU - 1; ed > 0; ed --) {
				if (w->hasEdit(ed)) {
					editWordList[ed].push_back(i->first);
					break;
				}
			}
#endif
#else
			if (w->hasEdit())
				editWordList[0].push_back(i->first);
#endif
		}
		// pthread_rwlock_unlock(&wordMapLock);
		// fprintf(stdout, "searching hamming/edit = %d/%d\n", perf_counter_hamming - old_perf_hamming, perf_counter_edit - old_perf_edit);
		// old_perf_hamming = perf_counter_hamming;
		// old_perf_edit = perf_counter_edit;
		hamming_vptree->create(hammingWordList);
#if ENABLE_SEPARATE_EDIT123
		for (int ed = 1; ed < TAU; ed++) {
#else
		for (int ed = 0; ed < 1; ed++) {
#endif

#if ENABLE_MULTI_EDITVPTREE
			// fprintf(stderr, "LEN ed=%d ", ed);
			for (int i = 1; i <= MAX_WORD_LENGTH; i++) {
				std::vector<mystring> editWordList2;
				for (std::vector<mystring>::iterator j = editWordList[ed].begin();
					j != editWordList[ed].end(); j++) {
					int len = j->length();
					if (i - ed <= len && len <= i + ed)
						editWordList2.push_back(*j);
				}
				// fprintf(stderr, "%d ", editWordList2.size());
				edit_vptree[ed][i]->create(editWordList2);
			}
			// fprintf(stderr, "\n");
#else
			edit_vptree[ed][0]->create(editWordList[ed]);
#endif
		}
		// fprintf(stdout, "indexing hamming/edit = %d/%d\n", perf_counter_hamming - old_perf_hamming, perf_counter_edit - old_perf_edit);
		// old_perf_hamming = perf_counter_hamming;
		// old_perf_edit = perf_counter_edit;

#if ENABLE_RESULT_CACHE
#if ENABLE_GLOBAL_RESULT_CACHE
		ASSERT_THREAD(MASTER_THREAD, 0);
		ASSERT_PHASE(PHASE_INDEX);
		// pthread_rwlock_wrlock(&resultCacheLock);
		for(ResultCache::iterator i = resultCache.begin();
			i != resultCache.end();
			i++ ) {
			DELETE(i->second);
		}
		resultCache.clear();
		// pthread_rwlock_unlock(&resultCacheLock);
#endif
#if ENABLE_THREAD_RESULT_CACHE
		for (int i = 0; i < thread_result_cache_n; i++) {
			__threadResultCache[i].clear();
		}
#endif
#endif
		long long end = GetClockTimeInUS();
		/* As we have the vpTreeLock, I can access the stats safely */
		ASSERT_THREAD(MASTER_THREAD, 0);
		global_perf_counter_index_hamming += perf_counter_hamming - old_perf_hamming;
		global_perf_counter_index_edit += perf_counter_edit - old_perf_edit;
		stats.total_indexing += end - start;
		stats.total_indexing_and_query_adding += GetClockTimeInUS() - stats.start_indexing_and_query_adding;
		// fprintf(logf, "[%lld.%06lld] end of indexing\n", end / 1000000LL % 86400, end % 1000000LL);

		return 1;
	}
	return 0;
}

static int clear_vptrees() {
	if (thread_type != MASTER_THREAD || thread_id != 0)
		return 0;

	if (hamming_vptree) {
		// long long now = GetClockTimeInUS();
		// fprintf(logf, "[%lld.%06lld] start of indexing\n", now / 1000000LL % 86400, now % 1000000LL);
		stats.start_indexing_and_query_adding = GetClockTimeInUS();
		DELETE(hamming_vptree);
		hamming_vptree = NULL;
#if ENABLE_SEPARATE_EDIT123
		for (int ed = 1; ed < TAU; ed ++) {
#else
		for (int ed = 0; ed < 1; ed ++) {
#endif
			for (int i = 0; i <= MAX_WORD_LENGTH; i++) {
				DELETE(edit_vptree[ed][i]);
				edit_vptree[ed][i] = NULL;
			}
		}
		return 1;
	}
	return 0;
}

ErrorCode VPTreeQueryAdd(QueryID query_id, const char* query_str, MatchType match_type, unsigned int match_dist) {
	clear_vptrees();
	Query* q = NEW(Query, query_id, query_str, match_type, match_dist);
	ASSERT_THREAD(MASTER_THREAD, 0);
	// pthread_rwlock_wrlock(&queryMapLock);
	queryMap.insert(std::pair<QueryID, Query*>(query_id, q));
	// pthread_rwlock_unlock(&queryMapLock);
	bool first = true;
	int i = 0;
	ITERATE_QUERY_WORDS(query_word, query_str) {
		mystring query_word_string = word_to_string(query_word);
		// pthread_rwlock_wrlock(&wordMapLock);
		ASSERT_PHASE(PHASE_SERIAL);
		ASSERT_THREAD(MASTER_THREAD, 0);
		WordMap::iterator found = wordMap.find(query_word_string);
		Word* word;
		if (found != wordMap.end()) {
			word = I2P(found->second);
			word->push_query(query_id, first, match_type, match_dist);
		}
		else {
			word = NEW(Word, query_word_string);
			word->push_query(query_id, first, match_type, match_dist);
			wordMap.insert(std::pair<mystring, Word*>(query_word_string, word));
			wordMapByID.insert(std::pair<WordIDType, Word*>(word->id(), word));
			// wordSet.insert(query_word_string);
		}
		// pthread_rwlock_unlock(&wordMapLock);
		if (i >= MAX_QUERY_WORDS)
		{
			fprintf(logf, "ERROR! exceed MAX_QUERY_WORDS\n");
		}
		q->word_ids[i] = word->id();
		i ++;
		first = false;
	}
	for (; i < MAX_QUERY_WORDS; i++) {
		q->word_ids[i] = -1;
	}
	return EC_SUCCESS;
}

ErrorCode VPTreeQueryRemove(QueryID query_id) {
	ASSERT_THREAD(MASTER_THREAD, 0);
	// pthread_rwlock_wrlock(&queryMapLock);
	QueryMap::iterator found = queryMap.find(query_id);
	clear_vptrees();
	if (found == queryMap.end()) {
		// pthread_rwlock_unlock(&queryMapLock);
		return EC_SUCCESS;
	}
	Query* query = I2P(found->second);
	queryMap.erase(found);
	// pthread_rwlock_unlock(&queryMapLock);
	bool first = true;
	ITERATE_QUERY_WORDS(query_word, query->getQueryStr()) {
		mystring query_word_string = word_to_string(query_word);
		// pthread_rwlock_wrlock(&wordMapLock);
		ASSERT_PHASE(PHASE_SERIAL);
		ASSERT_THREAD(MASTER_THREAD, 0);
		WordMap::iterator word_found = wordMap.find(query_word_string);
		if (word_found == wordMap.end()) {
			fprintf(logf, "ERROR: word not found for query(%s) and word(%s)\n", query->getQueryStr(), query_word_string.c_str());
			// pthread_rwlock_unlock(&wordMapLock);
			continue;
		}
		Word* word = I2P(word_found->second);
		word->remove_query(query_id, query->match_type, query->match_dist);
		// BUG: if the same query appears twice in a word?
		if (first)
			word->remove_first_word_query(query_id);
		if (word->empty()) {
			// dispose or delay disposing!
			// wordSet.erase(query_word_string);
			wordMap.erase(word_found);
			wordMapByID.erase(word->id());

			DELETE(word);
		}
		// pthread_rwlock_unlock(&wordMapLock);
		first = false;
	}
	DELETE(query);

	return EC_SUCCESS;
}

typedef std::set<WordIDType> SET;

static WordIDType* do_union_y(std::vector<mystring>* y) {
	WordIDType* x = (WordIDType*)MALLOC((y->size() + 1) * sizeof(WordIDType));
	if (x == NULL) {
		fprintf(logf, "cannot malloc\n");
		exit(1);
	}
	int j = 0;
	// pthread_rwlock_rdlock(&wordMapLock);
	for(std::vector<mystring>::iterator i = y->begin();
		i != y->end();
		i++, j++) {
		Word* w = I2P(wordMap.find(*i)->second);

		x[j] = w->id();
	}
	// pthread_rwlock_unlock(&wordMapLock);
	x[j] = -1;
	return x;
}

static void do_union_INT(SET* x, WordIDType* y) {
	for(WordIDType* p = y; *p >= 0; p++) {
		x->insert(*p);
	}
}

static void do_union_SET(SET* x, SET* y) {
	for(SET::iterator i = y->begin();
		i != y->end();
		i++) {

		x->insert(*i);
	}
}

void words_to_queries(SET* matchedHammingWords, SET* matchedEditWords, std::vector<QueryID>& query_ids) {
	for(SET::iterator i = matchedEditWords[3].begin();
		i != matchedEditWords[3].end();
		i++) {

		// pthread_rwlock_rdlock(&wordMapLock);
		Word* word = I2P(wordMapByID.find(*i)->second);
		// pthread_rwlock_unlock(&wordMapLock);
		for(std::set<QueryID>::iterator j = word->begin();
			j != word->end();
			j++) {

			bool match = true;

			QueryID query_id = *j;
			// pthread_rwlock_rdlock(&queryMapLock);
			Query* query = I2P(queryMap.find(query_id)->second);
			// pthread_rwlock_unlock(&queryMapLock);
			for (int j = 0; j < MAX_QUERY_WORDS && query->word_ids[j] != -1; j++) {
				WordIDType id = query->word_ids[j];
				// if query is hamming
				if ((query->match_type == MT_EXACT_MATCH &&
					! matchedHammingWords[0].count(id)) ||
				    (query->match_type == MT_HAMMING_DIST &&
					! matchedHammingWords[query->match_dist].count(id)) ||
				    (query->match_type == MT_EDIT_DIST &&
					! matchedEditWords[query->match_dist].count(id))) {

					match = false;
					break;
				}
			}

			if (match) {
				query_ids.push_back(query_id);
			}
		}
	}
}

ResultSet* findCachedResult(mystring doc_word_string) {
	ResultSet* rs = NULL;
#if ENABLE_RESULT_CACHE
	ResultCache::iterator found;
#if 0 && ENABLE_THREAD_RESULT_CACHE // I have changed the meaning of THREAD_RESULT_CACHE to WORD_WAITER's cache
	found = threadResultCache->find(doc_word_string);
	if(found != threadResultCache->end()) {
		rs = found->second;
		return rs;
	}
#endif
#if ENABLE_GLOBAL_RESULT_CACHE
	// ASSERT_PHASE(PHASE_WAIT_DOCS); OR ASSERT_PHASE(PHASE_ENQUEUE)
	// pthread_rwlock_rdlock(&resultCacheLock);
	found = resultCache.find(doc_word_string);

	if (found != resultCache.end()) {
		rs = found->second;
	}
	// pthread_rwlock_unlock(&resultCacheLock);

#if 0
	for (int i = 0; rs && i < TAU; i++) {
		unsigned long hex = (long)rs->results_hamming[i];
		if ((hex > 0x10000000 && ((hex & 0xffffffff00000000LL) != 0x2aaa00000000))) {
			fprintf(logf, "doc_word_string=[%s]\n", doc_word_string.c_str());
			int tryc = rs->results_hamming[i][0];
			fprintf(logf, "rs->results_hamming[%d] too big %p, %d\n", i,
					rs->results_hamming[i], tryc);
		}
	}
#endif
#endif
#endif
	return rs;
}

//////////////////////////////////////////////////////////////////////////////////////////////////

struct WordRequestResponse {
	mystring doc_word_string;
#if 0
#define SEARCH_HAMMING      1
#define SEARCH_EDIT         2
#define SEARCH_HAMMING_EDIT 3
	int searchtype; // 1 for hamming, 2 for edit; 3 for hamming+edit
	int tau; // TAU
#endif
	ResultSet* rs;
	// int waiting_doc_worker;
#if 0
	int processed_by;
#endif

	struct WordRequestResponse* next;
	struct WordRequestResponse* resp_next;
	char __padding[64 - sizeof(mystring) - sizeof(ResultSet*) - 2 * sizeof(struct WordRequestResponse*)];
};

#if 0
#define WRRN 24
#define REQ_N (WRRN)
#define RESP_N (WRRN * 20 + 1)
struct WordResponseRing {
	struct WordRequestResponse* resp[RESP_N];
	int doc_worker_id;
	int head;
	int tail;
	pthread_cond_t new_resp;
	pthread_cond_t resp_got;
	pthread_mutex_t lock;

	WordResponseRing(int doc_worker_id):
		doc_worker_id(doc_worker_id),
		head(0), tail(0)
	{
		pthread_cond_init(&this->new_resp, NULL);
		pthread_cond_init(&this->resp_got, NULL);
		pthread_mutex_init(&this->lock, NULL);
	}
};
#endif

int req_ring_n = REQ_RING_N;
int req_enqueue_batch = REQ_ENQUEUE_BATCH;

struct WordRequestRing {
	// struct WordRequestResponse* req[REQ_N];
	int ring_id;
	// int head;
	// int tail;
	struct WordRequestResponse* head;
	struct WordRequestResponse* tail;
	pthread_cond_t new_req;
	pthread_mutex_t lock;
	int exiting;

	WordRequestRing(int ring_id):
		ring_id(ring_id),
		head(0), tail(0)
	{
		pthread_cond_init(&this->new_req, NULL);
		pthread_mutex_init(&this->lock, NULL);
	}

	void append(WordRequestResponse* req) {
		if (tail == NULL) {
			head = tail = req;
		}
		else {
			tail->next = req;
			tail = req;
		}
		/* some req will bring some brothers */
		while (tail->next)
			tail = tail->next;
	}
};
int GetWaiterIDFromReqringID(int reqring_id) {
	// Refer to GetReqringToSend
	return reqring_id % word_waiter_n;
}
struct WordRequestResponseRing {
	WordRequestRing* reqring[REQ_RING_N];
	WordRequestResponse* resphead[WORD_FEEDER_N];
	WordRequestResponse* resptail[WORD_FEEDER_N];
	// WordResponseRing* respring[WORD_SEARCHER_N];
	int worker_threads;
	pthread_t pts[WORD_SEARCHER_N];
	pthread_mutex_t pts_lock;

	pthread_mutex_t lock;
	pthread_mutex_t resplock[WORD_FEEDER_N];
	pthread_cond_t respcond[WORD_FEEDER_N];

	WordRequestResponseRing(int req_ring_n):
		worker_threads(0)
	{
		pthread_mutex_init(&this->pts_lock, NULL);
		pthread_mutex_init(&this->lock, NULL);
		for (int i = 0; i < req_ring_n; i++)
			reqring[i] = NEW(WordRequestRing, i);
		for (int i = 0; i < WORD_FEEDER_N; i++) {
			pthread_mutex_init(&resplock[i], NULL);
			pthread_cond_init(&respcond[i], NULL);
			resphead[i] = resptail[i] = NULL;
		}
		// for (int i = 0; i < DOC_WORKER_N; i++)
		// 	respring[i] = NEW(WordResponseRing, i);
	}
};

struct WordSearcherArg {
	int tid;
	struct WordRequestResponseRing* ring;
	struct WordRequestRing* reqring;
};
void* WordSearcher(void* arg) {
	WordSearcherArg* wordSearcher = (WordSearcherArg*)arg;
	struct WordRequestResponseRing* ring = wordSearcher->ring;
	struct WordRequestRing* reqring = wordSearcher->reqring;
	setThread(WORD_SEARCHER_THREAD, wordSearcher->tid);
	DELETE(wordSearcher);

	StickToCores(WORD_SEARCHER_THREAD, thread_id, word_searcher_n);

	long long total_hamming_search = 0;
	long long total_edit_search = 0;

	const int BATCH = 8;

	// int counter = 0;

	pthread_mutex_lock(&reqring->lock);
	while (! reqring->exiting) {
		while (reqring->head == NULL && !reqring->exiting) {
			thread_fprintf(logf, "%d:%d waiting new_req /%d\n", thread_type, thread_id, reqring->ring_id);
			pthread_cond_wait(&reqring->new_req, &reqring->lock);
		}
		if (reqring->exiting)
			break;
		if (reqring->head == NULL) {
			fprintf(stderr, "error reqring->head == NULL\n");
			exit(1);
		}

		int batch = 0;
		struct WordRequestResponse* wrr = reqring->head; // reqring->req[reqring->head];
		struct WordRequestResponse* head = wrr;
		// reqring->head = (reqring->head + 1) % REQ_N;
		while (head != NULL && batch < BATCH) {
			head = head->next;
			batch ++;
		}
		reqring->head = head;
		if (head == NULL)
			reqring->tail = NULL;
		pthread_mutex_unlock(&reqring->lock);

#if 0
		int searchtype = wrr->searchtype;
		int tau = wrr->tau;
		if (tau != TAU) {
			fprintf(logf, "tau(%d) != TAU(%d)\n", tau, TAU);
			exit(1);
		}
#endif
		// int waiting_doc_worker = wrr->waiting_doc_worker;
REDO:
		mystring doc_word_string = wrr->doc_word_string;

		ResultSet* rs = NEW(ResultSet, );
		if (1) {
			std::vector<mystring> results[TAU];
			long long start = GetClockTimeInUS();
			hamming_vptree->search(doc_word_string, TAU, results);
			for (int i = 0; i < TAU; i++) {
				rs->results_hamming[i] = do_union_y(&results[i]);
			}
			total_hamming_search += GetClockTimeInUS() - start;
		}
		thread_fprintf(logf, "%d:%d got new_req\n", thread_type, thread_id);

		if (1) {
			std::vector<mystring> results[TAU];
			long long start = GetClockTimeInUS();
#if ENABLE_SEPARATE_EDIT123
			for (int ed = 1; ed < TAU; ed ++) {
				int tau = ed  + 1;
#else
			for (int ed = 0; ed < 1; ed ++) {
				int tau = TAU;
#endif
#if ENABLE_MULTI_EDITVPTREE
				int len = doc_word_string.length();
				edit_vptree[ed][len]->search(doc_word_string, tau, results);
#else
				edit_vptree[ed][0]->search(doc_word_string, tau, results);
#endif
			}
			for (int i = 0; i < TAU; i++) {
				rs->results_edit[i] = do_union_y(&results[i]);
			}
			total_edit_search += GetClockTimeInUS() - start;
		}
		wrr->rs = rs;

		batch --;
#if 0
		counter += 1;
		if (counter % 1024 == 0) {
			int cpuid = GetCPUID();
			fprintf(stderr, "%d:%d running on %d\n", thread_type, thread_id, cpuid);
#if 0
			cpu_set_t cpuset;
			pthread_getaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
			for (int j = 0; j < CPU_SETSIZE; j++)
				if (CPU_ISSET(j, &cpuset))
					printf("    CPU %d\n", j);
#endif

		}
#endif
		if (batch) {
			wrr = wrr->next;
			goto REDO;
		}
#if 0
		wrr->processed_by = thread_id;
#endif

#if 0
		WordResponseRing* respring = ring->respring[waiting_doc_worker];
		pthread_mutex_lock(&respring->lock);
		while ((respring->tail + 1) % RESP_N == respring->head) {
			thread_fprintf(logf, "%d:%d waiting resp_got\n", thread_type, thread_id);
			pthread_cond_wait(&respring->resp_got, &respring->lock);
		}
		respring->resp[respring->tail] = wrr;
		respring->tail = (respring->tail + 1) % RESP_N;
		thread_fprintf(logf, "%d:%d sending new_resp to %d\n",
				thread_type, thread_id, waiting_doc_worker);
		pthread_cond_signal(&respring->new_resp);
		pthread_mutex_unlock(&respring->lock);
#endif

		pthread_mutex_lock(&reqring->lock);

		/* Only notify when req is not pending */
		if (reqring->head == reqring->tail) {
			// fprintf(logf, "signaling respcond of %s\n", doc_word_string.c_str());
			int i = GetWaiterIDFromReqringID(reqring->ring_id);
			pthread_mutex_lock(&ring->resplock[i]);
			pthread_cond_signal(&ring->respcond[i]);
			// FIXME: use multiple singals instead
			pthread_mutex_unlock(&ring->resplock[i]);
		}
	}
	pthread_mutex_unlock(&reqring->lock);
	pthread_mutex_lock(&global_counter_lock);
	global_perf_counter_hamming += perf_counter_hamming;
	global_perf_counter_edit += perf_counter_edit;
	pthread_mutex_unlock(&global_counter_lock);
	ASSERT_THREAD(WORD_SEARCHER_THREAD, thread_id);
	fprintf(logf, "h/e w:%02d %lldM/%lldM %lld.%06llds/%lld.%06llds\n", thread_id,
			perf_counter_hamming/1000000LL, perf_counter_edit/1000000LL,
			total_hamming_search/1000000LL, total_hamming_search%1000000LL,
			total_edit_search/1000000LL, total_edit_search%1000000LL);
	perf_counter_hamming = 0;
	perf_counter_edit = 0;
	pthread_exit(NULL);
	return NULL;
}
#if 0
// Send request and possible return response.
struct WordRequestResponse* WaitSearchWordResponse(struct WordRequestResponseRing* ring) {
	WordRequestResponse* response = NULL;
	WordResponseRing* respring = ring->respring[thread_id];
	pthread_mutex_lock(&respring->lock);
	while (respring->head == respring->tail) {
		thread_fprintf(logf, "%d:%d waiting new_resp\n",
				thread_type, thread_id);
		pthread_cond_wait(&respring->new_resp, &respring->lock);
	}

	// assume only one document thread
	response = respring->resp[respring->head];
	respring->head = (respring->head + 1) % RESP_N;
	thread_fprintf(logf, "%d:%d resp got from %d\n", thread_type, thread_id, response->processed_by);
	pthread_cond_signal(&respring->resp_got);
	pthread_mutex_unlock(&respring->lock);
	return response;
}
#endif

#if 0
int FREQ[] = {0, 1, 2, 8, 12, 40, 50, 30,
		20, 10, 2, 1, 1, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0, 0};
int range[MAX_WORD_LENGTH + 1][3];
int ring_id_per_thread[WORD_SEARCHER_N];
int n_ranges = 0;
int len_to_ring_id[MAX_WORD_LENGTH + 1];
int DivideFrequency(int n) {
	int sum = 0;
	fprintf(logf, "n = %d\n", n);
	for (int i = 0; i <= MAX_WORD_LENGTH; i++) {
		sum += FREQ[i];
	}
	int per_thread = sum / n;
	int SUM = sum;
	fprintf(logf, "per_thread = %d, sum = %d\n", per_thread, sum);
	if (per_thread < 0) {
		per_thread = 1;
	}
	range[0][0] = 0;
	sum = 0;
	int threads = 0;
	int j = 0;
	for (int i = 0; i <= MAX_WORD_LENGTH; i ++) {
		if (j >= n - 1) {
			len_to_ring_id[i] = j;
			continue;
		}
		sum += FREQ[i];
		len_to_ring_id[i] = j;
		if (per_thread <= sum) {
			range[j][1] = i + 1;
			range[j][2] = sum * n / SUM;
			sum = 0;
			for (int k = threads; k != threads + range[j][2]; k++) {
				ring_id_per_thread[k] = j;
			}
			threads += range[j][2];
			j ++;
			range[j][0] = i + 1;
		}
	}
	range[j][1] = MAX_WORD_LENGTH + 1;
	range[j][2] = n - threads;
	for (int k = threads; k < n; k++) {
		ring_id_per_thread[k] = j;
	}
	j ++;
	for(int i = 0; i < j; i++) {
		fprintf(logf, "[%d, %d) x %d\n", range[i][0], range[i][1], range[i][2]);
	}
	if (n - threads <= 0) {
		fprintf(logf, "ERROR\n");
		exit(1);
	}

	return j;
}
#endif

int GetReqringIDFromWordSearcherID(int tid) {
	if (word_searcher_n % req_ring_n != 0) {
		fprintf(stderr, "word_searcher_n %% req_ring_n != 0\n");
		exit(1);
	}
	return tid / (word_searcher_n / req_ring_n);
}

void CreateWordSearchers(struct WordRequestResponseRing* ring, int n) {
	pthread_mutex_lock(&ring->pts_lock);
	while (ring->worker_threads < n) {
		WordSearcherArg* arg = NEW(WordSearcherArg, );
		int tid = ring->worker_threads;
		arg->tid = tid;
		arg->ring = ring;
		int reqring_id = GetReqringIDFromWordSearcherID(tid);
		arg->reqring = ring->reqring[reqring_id];
		int ret_val = pthread_create(&ring->pts[ring->worker_threads],
				NULL, WordSearcher, arg);
		if (ret_val != 0) {
			perror("Pthread create error!");
			exit(1);
		}
		ring->worker_threads += 1;
	}
	pthread_mutex_unlock(&ring->pts_lock);

}
#if 0
static int GetReqringToSend(struct WordRequestResponse* request) {
	return request->doc_word_string.length() % req_ring_n;
}
#endif
static int GetReqringToSend() {
	static __thread int round_robin = 0;
	// ASSERT_THREAD(WORD_FEEDER_THREAD, thread_id);
	ASSERT_THREAD(WORD_WAITER_THREAD, thread_id);
	if (req_ring_n % word_feeder_n != 0) {
		fprintf(stderr, "(req_ring_n %% word_feeder_n != 0)\n");
		exit(1);
	}
	round_robin = (round_robin + 1) % (req_ring_n / word_feeder_n);
	return round_robin * word_waiter_n + thread_id;
}
static struct WordRequestResponse* SendSearchWordRequest(struct WordRequestResponseRing* ring,
			int reqring_id,
			struct WordRequestResponse* request, int must) {

#if 0
#if REQ_ENQUEUE_BATCH == 1
	int reqring_id = GetReqringToSend(request);
#else
	int reqring_id = GetReqringToSend();
#endif
#endif
	if (reqring_id > req_ring_n) {
		fprintf(stderr, "(reqring_id > req_ring_n)\n");
		exit(1);
	}
	WordRequestRing* reqring = ring->reqring[reqring_id];
#if 1
	if (!must) {
		if (pthread_mutex_trylock(&reqring->lock) != 0)
			return request;
	}
#endif

	if (must) {
		if (pthread_mutex_lock(&reqring->lock) != 0)
			return request;
	}

	/* linked list, now no req limitation */
	reqring->append(request);
	thread_fprintf(logf, "%d:%d sending new_req to /%d\n", thread_type, thread_id, reqring_id);
	pthread_cond_signal(&reqring->new_req);
	pthread_mutex_unlock(&reqring->lock);
	return NULL;
}

//////////////////////////////////////////////////////////////////////////////////////////////////

struct WordRequestResponseRing* ring;
int word_searcher_n = WORD_SEARCHER_N;

#if 1
/* This part is called by master thread to presend requests to WordMatcher. */
struct SET_OF_STRING {
	std::set<mystring> bylen[MAX_WORD_LENGTH][26];

	void clear() {
		for (int i = 0; i < MAX_WORD_LENGTH; i++) {
			for (int j = 0; j < 26; j++) {
				this->bylen[i][j].clear();
			}
		}
	}
	void insert(const mystring& str) {
		this->bylen[str.length()][str.c_str()[0] - 'a'].insert(str);
	}
} batch_doc_words;
void BuildIndex() {
	if (new_vptrees_unless_exists()) {
		// fprintf(logf, "%d:%d Index built\n", thread_type, thread_id);
		batch_doc_words.clear();
	}
}
ErrorCode VPTreeMasterMatchDocument(DocID doc_id, const char* doc_str) {
	if (new_vptrees_unless_exists()) {
		// fprintf(logf, "%d:%d Index built\n", thread_type, thread_id);
		batch_doc_words.clear();
	}
#if 0
	const int BATCH = req_enqueue_batch;
	int batch = 0;
	WordRequestResponse* last_req = NULL;
#endif
	ITERATE_QUERY_WORDS(doc_word, doc_str) {
		mystring doc_word_string = word_to_string(doc_word);
#if 0
		if (batch_doc_words.count(doc_word_string))
			continue;
#endif
		batch_doc_words.insert(doc_word_string);
#if 0
		WordRequestResponse* request = NEW(WordRequestResponse, );
		// request->waiting_doc_worker = -1; // Master
		request->doc_word_string = doc_word_string;
#if 0
		request->searchtype = SEARCH_HAMMING_EDIT;
		request->tau = TAU;
#endif
		request->rs = NULL;
		request->next = last_req;
		request->resp_next = NULL;

		if(ring->resptail) {
			ring->resptail->resp_next = request;
			ring->resptail = request;
		}
		else {
			ring->resphead = ring->resptail = request;
		}
		batch ++;

		if (batch % BATCH >= 0) {
			last_req = SendSearchWordRequest(ring, request, 0);
			if (!last_req)
				batch = 0;
		}
		else {
			last_req = request;
		}
#endif
	}
#if 0
	if (batch)
		SendSearchWordRequest(ring, last_req, 1);
#endif

	return EC_SUCCESS;
}

int word_feeder_n = WORD_FEEDER_N;
ErrorCode VPTreeWordFeeder(int word_feeder_id) {
	int j = 0;
	const int BATCH = req_enqueue_batch;
	int batch = 0;
	WordRequestResponse* last_req = NULL;
	for (int len = 0; len != MAX_WORD_LENGTH; len ++) {
#if ENABLE_LEN_AWARE_REQRING
		if (len % word_feeder_n != word_feeder_id)
			continue;
	for (int c = 0; c < 26; c++) {
#endif
	for (std::set<mystring>::iterator i = batch_doc_words.bylen[len][c].begin();
					i != batch_doc_words.bylen[len][c].end(); i++, j++) {
#if ENABLE_LEN_AWARE_REQRING
		if (i->length() % word_feeder_n != word_feeder_id) {
			continue;
		}
#else
		if (j % word_feeder_n != word_feeder_id) {
			continue;
		}
#endif
#if 1
		WordRequestResponse* request = NEW(WordRequestResponse, );
		// request->waiting_doc_worker = -1; // Master
		request->doc_word_string = *i;
#if 0
		request->searchtype = SEARCH_HAMMING_EDIT;
		request->tau = TAU;
#endif
		request->rs = NULL;
		request->next = last_req;
		request->resp_next = NULL;

		if(ring->resptail[word_feeder_id]) {
			ring->resptail[word_feeder_id]->resp_next = request;
			ring->resptail[word_feeder_id] = request;
		}
		else {
			ring->resphead[word_feeder_id] = ring->resptail[word_feeder_id] = request;
		}
		batch ++;

		if (batch >= BATCH) {
			last_req = SendSearchWordRequest(ring,
				GetReqringToSend(), request, batch >= REQ_ENQUEUE_BATCH);
			if (!last_req)
				batch = 0;
		}
		else {
			last_req = request;
		}
#endif
	}
	}
	}
#if 1
	if (batch)
		SendSearchWordRequest(ring, GetReqringToSend(), last_req, 1);
#endif
	return EC_SUCCESS;
}

void WaitWordResults(int waiter_id) {
	ASSERT_THREAD(WORD_WAITER_THREAD, waiter_id);
	threadResultCache = &__threadResultCache[waiter_id];

	pthread_mutex_lock(&ring->resplock[waiter_id]);
	while (ring->resphead[waiter_id]) {
		while (ring->resphead[waiter_id] &&
			ring->resphead[waiter_id]->rs == NULL) {
			// fprintf(logf, "waiting for respcond of %s\n", ring->resphead[waiter_id]->doc_word_string.c_str());
			pthread_cond_wait(&ring->respcond[waiter_id], &ring->resplock[waiter_id]);
		}
		if (!ring->resphead[waiter_id])
			break;
		WordRequestResponse* response = ring->resphead[waiter_id];
		while ( ring->resphead[waiter_id] && ring->resphead[waiter_id]->rs != NULL) {
			ring->resphead[waiter_id] = ring->resphead[waiter_id]->resp_next;
		}
		if (ring->resphead[waiter_id] == NULL)
			ring->resptail[waiter_id] = NULL;
		pthread_mutex_unlock(&ring->resplock[waiter_id]);

		while (response != ring->resphead[waiter_id]) {
#if ENABLE_GLOBAL_RESULT_CACHE
			// ASSERT_THREAD(MASTER_THREAD, 0);
#if 0
	ResultSet* rs = response->rs;
	for (int i = 0; rs && i < TAU; i++) {
		unsigned long hex = (long)rs->results_hamming[i];
		if ((hex > 0x10000000 && ((hex & 0xffffffff00000000LL) != 0x2aaa00000000))) {
			fprintf(logf, "doc_word_string=[%s]\n", response->doc_word_string.c_str());
			int tryc = rs->results_hamming[i][0];
			fprintf(logf, "rs->results_hamming[%d] too big %p, %d\n", i,
					rs->results_hamming[i], tryc);
		}
	}
#endif
#if ENABLE_THREAD_RESULT_CACHE
			threadResultCache->insert(std::pair<mystring, ResultSet*>(
					response->doc_word_string, response->rs));
#else
			ERROR
#endif
#else
			ERROR
#endif
			response = response->resp_next;
		}

		pthread_mutex_lock(&ring->resplock[waiter_id]);
	}
	pthread_mutex_unlock(&ring->resplock[waiter_id]);
}
#endif

void MasterMergeCache() {
	for (int i = 0; i < word_waiter_n; i++) {
		resultCache.insert(__threadResultCache[i].begin(),
				__threadResultCache[i].end());
	}
}
ErrorCode VPTreeMatchDocument(DocID doc_id, const char* doc_str, std::vector<QueryID>& query_ids)
{
	if (new_vptrees_unless_exists()) {
		// fprintf(logf, "%d:%d Index built\n", thread_type, thread_id);
	}
	SET matchedHammingWords[TAU];
	SET matchedEditWords[TAU];
	std::set<mystring> docWords;
	// int in_flight = 0;

	long long start = GetClockTimeInUS();
	// fprintf(logf, "%d:%d Processing the results of %d\n", thread_type, thread_id, doc_id);
	// fprintf(logf, ".");

	/* Hopefully all the words are sent to word searchers in VPTreeMasterMatchDocument,
         * and their results are synthesized to resultCache */

	// for (const char* doc_word = doc_str; *doc_word /* || in_flight */;
	// 		doc_word = next_word_in_query(doc_word)) {
	ITERATE_QUERY_WORDS(doc_word, doc_str) {
		ResultSet* rs = NULL;
#if 0
		WordRequestResponse* response = NULL;
		WordRequestResponse* request = NULL;
#endif
		// if (*doc_word) {
			mystring doc_word_string = word_to_string(doc_word); // SPEED UP: question, I cannot reuse the pointer doc_str, but can I change *doc_str? */

			if (docWords.count(doc_word_string))
				continue;
			docWords.insert(doc_word_string);

			rs = findCachedResult(doc_word_string);

			if (rs == NULL) {
				fprintf(logf, "result of %s is not ready\n", doc_word_string.c_str());
				exit(1);
#if 0
				request = NEW(WordRequestResponse, );
				request->waiting_doc_worker = thread_id;
				request->doc_word_string = doc_word_string;
				request->searchtype = SEARCH_HAMMING_EDIT;
				request->tau = TAU;
#endif
			}
		// }

#if 0
resend_the_request:
		if (request) {
			mystring doc_word_string = word_to_string(doc_word);
			response = SendSearchWordRequest(ring, request);
			if (response == NULL) {
				request = NULL; // so that I do not resend the request
				in_flight += 1;
			}

		}
		else if (! *doc_word) {
			response = WaitSearchWordResponse(ring);
		}

		if (response) {
			in_flight -= 1;
			rs = response->rs;
#if ENABLE_RESULT_CACHE
			threadResultCache->insert(std::pair<mystring, ResultSet*>(response->doc_word_string, rs));
#endif
			DELETE(response);
			response = NULL;
		}
#endif

		if (rs) {
			for (int i = 0; i < TAU; i++) {
				do_union_INT(&matchedHammingWords[i], rs->results_hamming[i]);
			}
			for (int i = 0; i < TAU; i++)
				do_union_INT(&matchedEditWords[i], rs->results_edit[i]);
		}
#if 0
		if (request)
			goto resend_the_request;
#endif
	}
	// fprintf(stdout, "searching doc %d hamming/edit = %d/%d\n", doc_id, perf_counter_hamming - old_perf_hamming, perf_counter_edit - old_perf_edit);
#if 0
#if ENABLE_RESULT_CACHE
#if ENABLE_GLOBAL_RESULT_CACHE
	pthread_rwlock_wrlock(&resultCacheLock);
	resultCache.insert(threadResultCache->begin(), threadResultCache->end());
	pthread_rwlock_unlock(&resultCacheLock);
	threadResultCache->clear();
#endif
#endif
#endif

	for (int i = 1; i < TAU; i++)
		do_union_SET(&matchedHammingWords[i], &matchedHammingWords[i - 1]);

	for (int i = 1; i < TAU; i++)
		do_union_SET(&matchedEditWords[i], &matchedEditWords[i - 1]);

	for (int i = 0; i < TAU; i++)
		do_union_SET(&matchedEditWords[i], &matchedHammingWords[i]);

	words_to_queries(matchedHammingWords, matchedEditWords, query_ids);

	// performace: change iterator to const_iterator if possible.
	std::sort(query_ids.begin(), query_ids.end());

	thread_total_resultmerging += GetClockTimeInUS() - start;

	return EC_SUCCESS;
}

void vptree_doc_worker_init() {
#if ENABLE_THREAD_RESULT_CACHE
	ASSERT_THREAD(DOC_WORKER_THREAD, thread_id);
	// threadResultCache = &__threadResultCache[thread_id];
#endif
}

void vptree_doc_worker_destroy() {
	pthread_mutex_lock(&global_counter_lock);
	global_perf_counter_hamming += perf_counter_hamming;
	global_perf_counter_edit += perf_counter_edit;
	pthread_mutex_unlock(&global_counter_lock);
}

void vptree_system_init() {
	ASSERT_THREAD(MASTER_THREAD, 0);
	if (hku) {
		fprintf(logf, "system_init\n");
		fprintf(logf, "    TAU = %d\n", TAU);
		fprintf(logf, "    ENABLE_RESULT_CACHE        = %d\n", ENABLE_RESULT_CACHE);
		fprintf(logf, "    ENABLE_THREAD_RESULT_CACHE = %d\n", ENABLE_THREAD_RESULT_CACHE);
		fprintf(logf, "    ENABLE_GLOBAL_RESULT_CACHE = %d\n", ENABLE_GLOBAL_RESULT_CACHE);
		fprintf(logf, "\n");
		fprintf(logf, "    ENABLE_MULTI_EDITVPTREE    = %d\n", ENABLE_MULTI_EDITVPTREE);
		fprintf(logf, "    ENABLE_SEPARATE_EDIT123    = %d\n", ENABLE_SEPARATE_EDIT123);
		fprintf(logf, "\n");
		fprintf(logf, "    ENABLE_STATIC_MALLOC       = %d\n", ENABLE_STATIC_MALLOC);
		fprintf(logf, "    ENABLE_ALLOW_MEM_LEAK      = %d\n", ENABLE_ALLOW_MEM_LEAK);
		fprintf(logf, "\n");
		fprintf(logf, "    ENABLE_LEN_AWARE_REQRING   = %d\n", ENABLE_LEN_AWARE_REQRING);
		fprintf(logf, "    ENABLE_AFFINITY_SETTING    = %d\n", ENABLE_AFFINITY_SETTING);
		fprintf(logf, "\n");
		fprintf(logf, "    preset DOC_WORKER_N        = %d\n", DOC_WORKER_N);
		fprintf(logf, "    preset WORD_SEARCHER_N     = %d\n", WORD_SEARCHER_N);
		fprintf(logf, "    preset WORD_FEEDER_N       = %d\n", WORD_FEEDER_N);
		fprintf(logf, "    preset REQ_RING_N          = %d\n", REQ_RING_N);
	}

	char* env_word_searcher_n;
	if ((env_word_searcher_n = getenv("WORD_SEARCHER_N")) != NULL) {
		word_searcher_n = atoi(env_word_searcher_n);
	}
	fprintf(logf, "word_searcher_n = %d\n", word_searcher_n);
	char* env_word_feeder_n;
	if ((env_word_feeder_n = getenv("WORD_FEEDER_N")) != NULL) {
		word_feeder_n = atoi(env_word_feeder_n);
	}
	fprintf(logf, "word_feeder_n = %d\n", word_feeder_n);
	char* env_req_ring_n;
	if ((env_req_ring_n = getenv("REQ_RING_N")) != NULL) {
		req_ring_n = atoi(env_req_ring_n);
	}
	fprintf(logf, "req_ring_n = %d\n", req_ring_n);
	char* env_req_enqueue_batch;
	if ((env_req_enqueue_batch = getenv("REQ_ENQUEUE_BATCH")) != NULL) {
		req_enqueue_batch = atoi(env_req_enqueue_batch);
	}
	fprintf(logf, "req_enqueue_batch = %d\n", req_enqueue_batch);

	if (word_searcher_n < req_ring_n || word_searcher_n % req_ring_n != 0) {
		fprintf(logf, "word_searcher_n < req_ring_n or word_searcher_n %% req_ring_n != 0\n");
		exit(1);
	}
	if (req_ring_n > REQ_RING_N) {
		fprintf(logf, "req_ring_n > REQ_RING_N\n");
		exit(1);
	}
	if (word_searcher_n > WORD_SEARCHER_N) {
		fprintf(logf, "word_searcher_n > WORD_SEARCHER_N\n");
		exit(1);
	}
	if (word_feeder_n > WORD_FEEDER_N) {
		fprintf(logf, "word_feeder_n > WORD_FEEDER_N\n");
		exit(1);
	}

	ring = NEW(WordRequestResponseRing, req_ring_n);

	CreateWordSearchers(ring, word_searcher_n);
}

void vptree_system_destroy() {
	ASSERT_THREAD(MASTER_THREAD, 0);
	if (hku)
		fprintf(logf, "system_destroy\n");
	for (int reqring_id = 0; reqring_id < req_ring_n; reqring_id ++) {
		WordRequestRing* reqring = ring->reqring[reqring_id];
		pthread_mutex_lock(&reqring->lock);
		reqring->exiting = 1;
		pthread_cond_broadcast(&reqring->new_req);
		pthread_mutex_unlock(&reqring->lock);
	}

	for (int i = 0; i < word_searcher_n; i++) {
		pthread_join(ring->pts[i], NULL);
	}
	fprintf(logf, "G_hamming = %lld M\n", global_perf_counter_hamming / 1000000LL);
	fprintf(logf, "G_edit = %lld M\n", global_perf_counter_edit / 1000000LL);

	fprintf(logf, "G_index_hamming = %lld M\n", global_perf_counter_index_hamming / 1000000LL);
	fprintf(logf, "G_index_edit = %lld M\n", global_perf_counter_index_edit / 1000000LL);

	DELETE(ring);
}
