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

#define NON_NULL(a) ((*(a)) && ((*(a)) != ' '))

unsigned int HammingDistance(const char* a, int na, const char* b, int nb);
int EditDistance(const char* a, int na, const char* b, int nb);

#define TAU 4

/* Iterator to pointer */
#define I2P(x) (&(*(x)))

#define ENABLE_RESULT_CACHE 1
#define ENABLE_GLOBAL_RESULT_CACHE 0

#define ENABLE_MULTI_EDITVPTREE 1

static __thread int perf_counter_hamming = 0;
static __thread int perf_counter_edit = 0;
pthread_mutex_t global_counter_lock = PTHREAD_MUTEX_INITIALIZER;
int global_perf_conter_hamming = 0;
int global_perf_conter_edit = 0;
// must be int
int hamming(const std::string& a, const std::string& b) {
	unsigned int oo = 0x7FFFFFFF;
	unsigned int dist = HammingDistance(a.c_str(), a.length(), b.c_str(), b.length());
	perf_counter_hamming += 1;
	if (dist == oo) {
		return oo - TAU;
	}
	return dist;
}

int edit(const std::string& a, const std::string& b) {
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
	int word_ids[MAX_QUERY_WORDS];
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
// static int max_word_id = 0;

pthread_rwlock_t wordLock = PTHREAD_RWLOCK_INITIALIZER;
__thread int thread_max_word_id = 0;
class Word {
	std::string word;
	int hamming_queries;
	int edit_queries;
	std::set<QueryID> first_word_queries;
	int word_id;

public:
	Word(std::string word)
		: word(word), hamming_queries(0), edit_queries(0), first_word_queries() {

		// pthread_mutex_lock(&max_word_id_lock);
		word_id = thread_max_word_id * DOC_WORKER_N + thread_id;
		thread_max_word_id += 1;
		// pthread_mutex_unlock(&max_word_id_lock);
	}
	void push_query(QueryID q, bool first, MatchType match_type) {
		pthread_rwlock_wrlock(&wordLock);
		if (match_type == MT_HAMMING_DIST || match_type == MT_EXACT_MATCH)
			this->hamming_queries += 1;
		if (match_type == MT_EDIT_DIST)
			this->edit_queries += 1;
		if (first)
			this->first_word_queries.insert(q);
		pthread_rwlock_unlock(&wordLock);
	}
	void remove_query(QueryID q, MatchType match_type) {
		pthread_rwlock_wrlock(&wordLock);
		if (match_type == MT_HAMMING_DIST || match_type == MT_EXACT_MATCH)
			this->hamming_queries -= 1;
		if (match_type == MT_EDIT_DIST)
			this->edit_queries -= 1;
		pthread_rwlock_unlock(&wordLock);
	}
	void remove_first_word_query(QueryID q) {
		pthread_rwlock_wrlock(&wordLock);
		this->first_word_queries.erase(q);
		pthread_rwlock_unlock(&wordLock);
	}
	std::set<QueryID>::iterator begin() const{
		return first_word_queries.begin();
	}
	std::set<QueryID>::iterator end() const{
		return first_word_queries.end();
	}
	int id() const {
		return word_id;
	}
	bool empty() const {
		return this->hamming_queries == 0 && this->edit_queries == 0;
	}
	bool hasHamming() const {
		return !! this->hamming_queries;
	}
	bool hasEdit() const {
		return !! this->edit_queries;
	}
};

typedef std::map<std::string, Word*> WordMap;
typedef std::map<int, Word*> WordMapByID;
pthread_rwlock_t wordMapLock = PTHREAD_RWLOCK_INITIALIZER;
WordMap wordMap;
WordMapByID wordMapByID;
// std::set<std::string> wordSet;
typedef VpTree<std::string, int, hamming> HammingVpTree;
typedef VpTree<std::string, int, edit> EditVpTree;
pthread_rwlock_t vpTreeLock = PTHREAD_RWLOCK_INITIALIZER;
HammingVpTree* hamming_vptree;
EditVpTree* edit_vptree[MAX_WORD_LENGTH];

typedef std::map<QueryID, Query*> QueryMap;
pthread_rwlock_t queryMapLock = PTHREAD_RWLOCK_INITIALIZER;
QueryMap queryMap;


class ResultSet {
public:
	int* results_hamming[TAU];
	int* results_edit[TAU];
	~ResultSet() {
		for(int i = 0; i < TAU; i++)
			free(results_hamming[i]);
		for(int i = 0; i < TAU; i++)
			free(results_edit[i]);
	}
};

#if ENABLE_RESULT_CACHE
typedef std::map<std::string, ResultSet*> ResultCache;
#if ENABLE_GLOBAL_RESULT_CACHE
pthread_rwlock_t resultCacheLock = PTHREAD_RWLOCK_INITIALIZER;
ResultCache resultCache;
#endif
ResultCache __threadResultCache[DOC_WORKER_N];
__thread ResultCache* threadResultCache;
#endif

static const char* next_word_in_query(const char* query_str) {
	while (NON_NULL(query_str))
		query_str ++;
	while (*query_str == ' ')
		query_str ++;
	return query_str;
}

static std::string word_to_string(const char* word) {
	char w[MAX_WORD_LENGTH + 1];
	char* p = w;
	while(NON_NULL(word)) {
		*p = *word;
		word++;
		p++;
	}
	*p = 0;
	return std::string(w);
}

#define ITERATE_QUERY_WORDS(key, begin) for (const char* (key) = (begin); *(key); (key) = next_word_in_query((key)))

// int old_perf_hamming;
// int old_perf_edit;
static void new_vptrees_unless_exists() {
	std::vector<std::string> hammingWordList;
	std::vector<std::string> editWordList;
	pthread_rwlock_rdlock(&vpTreeLock);
	if (! hamming_vptree) {
		pthread_rwlock_unlock(&vpTreeLock);

		pthread_rwlock_wrlock(&vpTreeLock);
		if (hamming_vptree) {
			pthread_rwlock_unlock(&vpTreeLock);
			return;
		}
		long long start = GetClockTimeInUS();
		hamming_vptree = new HammingVpTree();
		for (int i = 0; i < MAX_WORD_LENGTH; i++) {
			edit_vptree[i] = new EditVpTree();
		}
		pthread_rwlock_rdlock(&wordMapLock);
		for(std::map<std::string, Word*>::iterator i = wordMap.begin();
			i != wordMap.end(); i++) {

			Word* w = I2P(i->second);

			if (w->hasHamming())
				hammingWordList.push_back(i->first);
			if (w->hasEdit())
				editWordList.push_back(i->first);
		}
		pthread_rwlock_unlock(&wordMapLock);
		// fprintf(stdout, "searching hamming/edit = %d/%d\n", perf_counter_hamming - old_perf_hamming, perf_counter_edit - old_perf_edit);
		// old_perf_hamming = perf_counter_hamming;
		// old_perf_edit = perf_counter_edit;
		hamming_vptree->create(hammingWordList);
#if ENABLE_MULTI_EDITVPTREE
		for (int i = 1; i < MAX_WORD_LENGTH; i++) {
			std::vector<std::string> editWordList2;
			for (std::vector<std::string>::iterator j = editWordList.begin();
				j != editWordList.end(); j++) {
				int len = j->length();
				if (i - TAU < len && len < i + TAU)
					editWordList2.push_back(*j);
			}
			edit_vptree[i]->create(editWordList2);
		}
#else
		edit_vptree[0]->create(editWordList);
#endif
		// fprintf(stdout, "indexing hamming/edit = %d/%d\n", perf_counter_hamming - old_perf_hamming, perf_counter_edit - old_perf_edit);
		// old_perf_hamming = perf_counter_hamming;
		// old_perf_edit = perf_counter_edit;

#if ENABLE_RESULT_CACHE
#if ENABLE_GLOBAL_RESULT_CACHE
		pthread_rwlock_wrlock(&resultCacheLock);
		for(ResultCache::iterator i = resultCache.begin();
			i != resultCache.end();
			i++ ) {
			delete i->second;
		}
		resultCache.clear();
		pthread_rwlock_unlock(&resultCacheLock);
#endif
		for (int i = 0; i < doc_worker_n; i++) {
			__threadResultCache[i].clear();
		}
#endif
		long long end = GetClockTimeInUS();
		/* As we have the vpTreeLock, I can access the stats safely */
		stats.total_indexing += end - start;
		stats.total_indexing_and_query_adding = GetClockTimeInUS() - stats.start_indexing_and_query_adding;
		stats.start_parallel = GetClockTimeInUS();

	}
	pthread_rwlock_unlock(&vpTreeLock);
}

static void clear_vptrees() {
	pthread_rwlock_rdlock(&vpTreeLock);
	if (hamming_vptree) {
		pthread_rwlock_unlock(&vpTreeLock);
		pthread_rwlock_wrlock(&vpTreeLock);
		if (! hamming_vptree) {
			pthread_rwlock_unlock(&vpTreeLock);
			return;
		}
		stats.total_parallel += GetClockTimeInUS() - stats.start_parallel;
		stats.start_indexing_and_query_adding = GetClockTimeInUS();
		delete hamming_vptree;
		hamming_vptree = NULL;
		for (int i = 0; i < MAX_WORD_LENGTH; i++) {
			delete edit_vptree[i];
			edit_vptree[i] = NULL;
		}
	}
	pthread_rwlock_unlock(&vpTreeLock);
}

ErrorCode VPTreeQueryAdd(QueryID query_id, const char* query_str, MatchType match_type, unsigned int match_dist) {
	clear_vptrees();
	Query* q = new Query(query_id, query_str, match_type, match_dist);
	pthread_rwlock_wrlock(&queryMapLock);
	queryMap.insert(std::pair<QueryID, Query*>(query_id, q));
	pthread_rwlock_unlock(&queryMapLock);
	bool first = true;
	int i = 0;
	ITERATE_QUERY_WORDS(query_word, query_str) {
		std::string query_word_string = word_to_string(query_word);
		pthread_rwlock_wrlock(&wordMapLock);
		WordMap::iterator found = wordMap.find(query_word_string);
		Word* word;
		if (found != wordMap.end()) {
			word = I2P(found->second);
			word->push_query(query_id, first, match_type);
		}
		else {
			word = new Word(query_word_string);
			word->push_query(query_id, first, match_type);
			wordMap.insert(std::pair<std::string, Word*>(query_word_string, word));
			wordMapByID.insert(std::pair<int, Word*>(word->id(), word));
			// wordSet.insert(query_word_string);
		}
		pthread_rwlock_unlock(&wordMapLock);
		if (i >= MAX_QUERY_WORDS)
		{
			fprintf(stderr, "ERROR! exceed MAX_QUERY_WORDS\n");
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
	pthread_rwlock_wrlock(&queryMapLock);
	QueryMap::iterator found = queryMap.find(query_id);
	clear_vptrees();
	if (found == queryMap.end()) {
		pthread_rwlock_unlock(&queryMapLock);
		return EC_SUCCESS;
	}
	Query* query = I2P(found->second);
	queryMap.erase(found);
	pthread_rwlock_unlock(&queryMapLock);
	bool first = true;
	ITERATE_QUERY_WORDS(query_word, query->getQueryStr()) {
		std::string query_word_string = word_to_string(query_word);
		pthread_rwlock_wrlock(&wordMapLock);
		WordMap::iterator word_found = wordMap.find(query_word_string);
		if (word_found == wordMap.end()) {
			fprintf(stderr, "ERROR: word not found for query(%s) and word(%s)\n", query->getQueryStr(), query_word_string.c_str());
			pthread_rwlock_unlock(&wordMapLock);
			continue;
		}
		Word* word = I2P(word_found->second);
		word->remove_query(query_id, query->match_type);
		// BUG: if the same query appears twice in a word?
		if (first)
			word->remove_first_word_query(query_id);
		if (word->empty()) {
			// dispose or delay disposing!
			// wordSet.erase(query_word_string);
			wordMap.erase(word_found);
			wordMapByID.erase(word->id());

			delete word;
		}
		pthread_rwlock_unlock(&wordMapLock);
		first = false;
	}
	delete query;

	return EC_SUCCESS;
}

typedef std::set<int> SET;

static int* do_union_y(std::vector<std::string>* y) {
	int* x = (int*)malloc(y->size() * sizeof(int) + 1);
	int j = 0;
	// pthread_rwlock_rdlock(&wordMapLock);
	for(std::vector<std::string>::iterator i = y->begin();
		i != y->end();
		i++, j++) {
		Word* w = I2P(wordMap.find(*i)->second);

		x[j] = w->id();
	}
	// pthread_rwlock_unlock(&wordMapLock);
	x[j] = -1;
	return x;
}

static void do_union(SET* x, int** y) {
	for(int* p = *y; *p >=0; p++) {
		x->insert(*p);
	}
}

static void do_union(SET* x, SET* y) {
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
				int id = query->word_ids[j];
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

ResultSet* findCachedResult(std::string doc_word_string) {
	ResultSet* rs = NULL;
#if ENABLE_RESULT_CACHE
	ResultCache::iterator found;
	found = threadResultCache->find(doc_word_string);
	if(found != threadResultCache->end())
		return found->second;

#if ENABLE_GLOBAL_RESULT_CACHE
	pthread_rwlock_rdlock(&resultCacheLock);
	found = resultCache.find(doc_word_string);

	if (found != resultCache.end()) {
		rs = found->second;
	}
	pthread_rwlock_unlock(&resultCacheLock);
#endif
#endif
	return rs;
}

//////////////////////////////////////////////////////////////////////////////////////////////////

struct WordRequestResponse {
	std::string doc_word_string;
#define SEARCH_HAMMING      1
#define SEARCH_EDIT         2
#define SEARCH_HAMMING_EDIT 3
	int searchtype; // 1 for hamming, 2 for edit; 3 for hamming+edit
	int tau; // TAU
	ResultSet* rs;
	int waiting_doc_worker;
};

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
		head(0), tail(0),
		doc_worker_id(doc_worker_id)
	{
		pthread_cond_init(&this->new_resp, NULL);
		pthread_cond_init(&this->resp_got, NULL);
		pthread_mutex_init(&this->lock, NULL);
	}
};
struct WordRequestResponseRing {
	struct WordRequestResponse* req[REQ_N];
	WordResponseRing* respring[WORD_SEARCHER_N];
	int reqhead;
	int reqtail;
	int worker_threads;
	pthread_t pts[WORD_SEARCHER_N];
	pthread_mutex_t pts_lock;

	pthread_mutex_t lock;
	pthread_cond_t new_req;
	pthread_cond_t req_got;

	// pthread_cond_t new_resp[DOC_WORKER_N];
	// pthread_cond_t resp_got;
	pthread_cond_t resp_cond;

	WordRequestResponseRing():
		reqhead(0), reqtail(0),
		worker_threads(0)
	{
		pthread_mutex_init(&this->pts_lock, NULL);
		pthread_mutex_init(&this->lock, NULL);
		pthread_cond_init(&this->new_req, NULL);
		pthread_cond_init(&this->req_got, NULL);
		for (int i = 0; i < DOC_WORKER_N; i++)
			respring[i] = new WordResponseRing(i);
		// for(int i = 0; i < DOC_WORKER_N; i++)
		// 	pthread_cond_init(&this->new_resp[i], NULL);
		// pthread_cond_init(&this->resp_got, NULL);
		pthread_cond_init(&this->resp_cond, NULL);
	}
};

struct WordSearcherArg {
	int tid;
	struct WordRequestResponseRing* ring;
};
void* WordSearcher(void* arg) {
	WordSearcherArg* wordSearcher = (WordSearcherArg*)arg;
	struct WordRequestResponseRing* ring = (struct WordRequestResponseRing*)wordSearcher->ring;
	thread_type = WORD_SEARCHER_THREAD;
	thread_id = wordSearcher->tid;
	delete wordSearcher;
	pthread_mutex_lock(&ring->lock);
	while (1) {
		while (ring->reqhead == ring->reqtail) {
			pthread_cond_wait(&ring->new_req, &ring->lock);
		}

		struct WordRequestResponse* wrr = ring->req[ring->reqhead];
		if (wrr == NULL) {
			break;
		}
		ring->reqhead = (ring->reqhead + 1) % WRRN;
		pthread_cond_signal(&ring->req_got);
		int searchtype = wrr->searchtype;
		int tau = wrr->tau;
		int waiting_doc_worker = wrr->waiting_doc_worker;
		std::string doc_word_string = wrr->doc_word_string;
		pthread_mutex_unlock(&ring->lock);

		ResultSet* rs = new ResultSet();
		if (searchtype & SEARCH_HAMMING) {
			std::vector<std::string> results[tau];
			hamming_vptree->search(doc_word_string, tau, results);
			for (int i = 0; i < tau; i++) {
				rs->results_hamming[i] = do_union_y(&results[i]);
			}
		}

		if (searchtype & SEARCH_EDIT) {
			std::vector<std::string> results[tau];
#if ENABLE_MULTI_EDITVPTREE
			int len = doc_word_string.length();
			edit_vptree[len]->search(doc_word_string, tau, results);
#else
			edit_vptree[0]->search(doc_word_string, tau, results);
#endif
			for (int i = 0; i < tau; i++) {
				rs->results_edit[i] = do_union_y(&results[i]);
			}
		}
		wrr->rs = rs;

		WordResponseRing* respring = ring->respring[waiting_doc_worker];
		pthread_mutex_lock(&respring->lock);
		while ((respring->tail + 1) % WRRN == respring->head) {
			pthread_cond_wait(&respring->resp_got, &respring->lock);
		}
		respring->resp[respring->tail] = wrr;
		respring->tail = (respring->tail + 1) % WRRN;
		pthread_cond_signal(&respring->new_resp);
		pthread_mutex_unlock(&respring->lock);

		pthread_mutex_lock(&ring->lock);
		pthread_cond_signal(&ring->req_got);
	}
	pthread_mutex_unlock(&ring->lock);
	pthread_mutex_lock(&global_counter_lock);
	global_perf_conter_hamming += perf_counter_hamming;
	global_perf_conter_edit += perf_counter_edit;
	pthread_mutex_unlock(&global_counter_lock);
	
}
// Send request and possible return response.
struct WordRequestResponse* WaitSearchWordResponse(struct WordRequestResponseRing* ring) {
	WordRequestResponse* response = NULL;
	WordResponseRing* respring = ring->respring[thread_id];
	pthread_mutex_lock(&respring->lock);
	while (respring->head == respring->tail) {
		pthread_cond_wait(&respring->new_resp, &respring->lock);
	}

	// assume only one document thread
	response = respring->resp[respring->head];
	respring->head = (respring->head + 1) % WRRN;
	pthread_cond_signal(&respring->resp_got);
	pthread_mutex_unlock(&respring->lock);
	return response;
}

void CreateWordSearchers(struct WordRequestResponseRing* ring, int n) {
	pthread_mutex_lock(&ring->pts_lock);
	while (ring->worker_threads < n) {
		WordSearcherArg* arg = new WordSearcherArg();
		arg->tid = ring->worker_threads;
		arg->ring = ring;
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
struct WordRequestResponse* SendSearchWordRequest(struct WordRequestResponseRing* ring,
			struct WordRequestResponse* request) {

	WordRequestResponse* response = NULL;
	pthread_mutex_lock(&ring->lock);
	WordResponseRing* respring = ring->respring[thread_id];
	while ((ring->reqtail + 1) % WRRN == ring->reqhead) {
		// assume only one document thread
		while ((response != NULL || respring->head == respring->tail) &&
			(ring->reqtail + 1) % WRRN == ring->reqhead) {
			pthread_cond_wait(&ring->req_got, &ring->lock);
		}
		if ((ring->reqtail + 1) % WRRN == ring->reqhead) {
			pthread_mutex_unlock(&ring->lock);
			pthread_mutex_lock(&respring->lock);
			if (response == NULL && respring->head != respring->tail) {
				response = respring->resp[respring->head];
				respring->head = (respring->head + 1) % WRRN;
				pthread_cond_signal(&respring->resp_got);
				pthread_mutex_unlock(&respring->lock);
				return response;
			}
			pthread_mutex_lock(&ring->lock);
		}

	}
	ring->req[ring->reqtail] = request;
	ring->reqtail = (ring->reqtail + 1) % WRRN;
	pthread_cond_signal(&ring->new_req);
	pthread_mutex_unlock(&ring->lock);

	return NULL;
}

//////////////////////////////////////////////////////////////////////////////////////////////////

struct WordRequestResponseRing* ring;
int word_searcher_n = WORD_SEARCHER_N;

ErrorCode VPTreeMatchDocument(DocID doc_id, const char* doc_str, std::vector<QueryID>& query_ids)
{
	new_vptrees_unless_exists();
	SET matchedHammingWords[TAU];
	SET matchedEditWords[TAU];
	std::set<std::string> docWords;
	int in_flight = 0;

	// int old_perf_hamming = perf_counter_hamming;
	// int old_perf_edit = perf_counter_edit;
	const char* doc_word = doc_str;

	for (const char* doc_word = doc_str; *doc_word || in_flight;
			doc_word = next_word_in_query(doc_word)) {
		ResultSet* rs = NULL;
		WordRequestResponse* response = NULL;
		WordRequestResponse* request = NULL;
		if (*doc_word) {
			std::string doc_word_string = word_to_string(doc_word); // SPEED UP: question, I cannot reuse the pointer doc_str, but can I change *doc_str? */

			if (docWords.count(doc_word_string))
				continue;
			docWords.insert(doc_word_string);

			rs = findCachedResult(doc_word_string);

			if (rs == NULL) {
				request = new WordRequestResponse();
				request->waiting_doc_worker = thread_id;
				request->doc_word_string = doc_word_string;
				request->searchtype = SEARCH_HAMMING_EDIT;
				request->tau = TAU;
			}
		}

resend_the_request:
		if (request) {
			std::string doc_word_string = word_to_string(doc_word);
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
			threadResultCache->insert(std::pair<std::string, ResultSet*>(response->doc_word_string, rs));
#endif
			delete response;
			response = NULL;
		}

		if (rs) {
			for (int i = 0; i < TAU; i++)
				do_union(&matchedHammingWords[i], &rs->results_hamming[i]);
			for (int i = 0; i < TAU; i++)
				do_union(&matchedEditWords[i], &rs->results_edit[i]);
		}
		if (request)
			goto resend_the_request;
	}
	long long start = GetClockTimeInUS();
	// fprintf(stdout, "searching doc %d hamming/edit = %d/%d\n", doc_id, perf_counter_hamming - old_perf_hamming, perf_counter_edit - old_perf_edit);
#if ENABLE_RESULT_CACHE
#if ENABLE_GLOBAL_RESULT_CACHE
	pthread_rwlock_wrlock(&resultCacheLock);
	resultCache.insert(threadResultCache->begin(), threadResultCache->end());
	pthread_rwlock_unlock(&resultCacheLock);
	threadResultCache->clear();
#endif
#endif

	for (int i = 1; i < TAU; i++)
		do_union(&matchedHammingWords[i], &matchedHammingWords[i - 1]);

	for (int i = 1; i < TAU; i++)
		do_union(&matchedEditWords[i], &matchedEditWords[i - 1]);

	for (int i = 0; i < TAU; i++)
		do_union(&matchedEditWords[i], &matchedHammingWords[i]);

	words_to_queries(matchedHammingWords, matchedEditWords, query_ids);

	// performace: change iterator to const_iterator if possible.
	std::sort(query_ids.begin(), query_ids.end());

	thread_total_resultmerging += GetClockTimeInUS() - start;

	return EC_SUCCESS;
}

void vptree_doc_worker_init() {
#if ENABLE_RESULT_CACHE
	threadResultCache = &__threadResultCache[thread_id];
#endif
}

void vptree_doc_worker_destroy() {
	pthread_mutex_lock(&global_counter_lock);
	global_perf_conter_hamming += perf_counter_hamming;
	global_perf_conter_edit += perf_counter_edit;
	pthread_mutex_unlock(&global_counter_lock);
}

void vptree_system_init() {
	ring = new WordRequestResponseRing();

	char* env_word_searcher_n;
	if ((env_word_searcher_n = getenv("WORD_SEARCHER_N")) != NULL) {
		word_searcher_n = atoi(env_word_searcher_n);
	}
	fprintf(stderr, "word_searcher_n = %d\n", word_searcher_n);
	if (word_searcher_n > WORD_SEARCHER_N) {
		fprintf(stderr, "word_searcher_n > WORD_SEARCHER_N\n");
		exit(1);
	}

	CreateWordSearchers(ring, word_searcher_n);
}

void vptree_system_destroy() {
	pthread_mutex_lock(&ring->lock);
	ring->reqhead = 0;
	ring->reqtail = word_searcher_n;
	for (int i = 0; i < word_searcher_n; i++) {
		ring->req[i] = NULL;
	}
	pthread_cond_broadcast(&ring->new_req);
	pthread_mutex_unlock(&ring->lock);

	for (int i = 0; i < word_searcher_n; i++) {
		pthread_join(ring->pts[i], NULL);
	}
	fprintf(stderr, "global_perf_conter_hamming = %d\n", global_perf_conter_hamming);
	fprintf(stderr, "global_perf_conter_edit = %d\n", global_perf_conter_edit);

	delete ring;
}
