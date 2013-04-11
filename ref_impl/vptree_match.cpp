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

#define NON_NULL(a) ((*(a)) && ((*(a)) != ' '))

unsigned int HammingDistance(const char* a, int na, const char* b, int nb);
int EditDistance(const char* a, int na, const char* b, int nb);

#define TAU 4

/* Iterator to pointer */
#define I2P(x) (&(*(x)))

static int perf_counter_hamming = 0;
static int perf_counter_edit = 0;
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

pthread_mutex_t max_word_id_lock = PTHREAD_MUTEX_INITIALIZER;
static int max_word_id = 0;

pthread_rwlock_t wordLock = PTHREAD_RWLOCK_INITIALIZER;
class Word {
	std::string word;
	int hamming_queries;
	int edit_queries;
	std::set<QueryID> first_word_queries;
	int word_id;

public:
	Word(std::string word)
		: word(word), hamming_queries(0), edit_queries(0), first_word_queries() {

		pthread_mutex_lock(&max_word_id_lock);
		word_id = max_word_id;
		max_word_id += 1;
		pthread_mutex_unlock(&max_word_id_lock);
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
EditVpTree* edit_vptree;

typedef std::map<QueryID, Query*> QueryMap;
pthread_mutex_t queryMapLock = PTHREAD_MUTEX_INITIALIZER;
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

typedef std::map<std::string, ResultSet*> ResultCache;
pthread_mutex_t resultCacheLock = PTHREAD_MUTEX_INITIALIZER;
ResultCache resultCache;

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

int old_perf_hamming;
int old_perf_edit;
static void new_vptrees_unless_exists() {
	std::vector<std::string> hammingWordList;
	std::vector<std::string> editWordList;
	pthread_rwlock_wrlock(&vpTreeLock);
	if (! hamming_vptree) {
		hamming_vptree = new HammingVpTree();
		edit_vptree = new EditVpTree();
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
		fprintf(stdout, "searching hamming/edit = %d/%d\n", perf_counter_hamming - old_perf_hamming, perf_counter_edit - old_perf_edit);
		old_perf_hamming = perf_counter_hamming;
		old_perf_edit = perf_counter_edit;
		hamming_vptree->create(hammingWordList);
		edit_vptree->create(editWordList);
		fprintf(stdout, "indexing hamming/edit = %d/%d\n", perf_counter_hamming - old_perf_hamming, perf_counter_edit - old_perf_edit);
		old_perf_hamming = perf_counter_hamming;
		old_perf_edit = perf_counter_edit;

		pthread_mutex_lock(&resultCacheLock);
		for(ResultCache::iterator i = resultCache.begin();
			i != resultCache.end();
			i++ ) {
			delete i->second;
		}
		resultCache.clear();
		pthread_mutex_unlock(&resultCacheLock);
	}
	pthread_rwlock_unlock(&vpTreeLock);
}

static void clear_vptrees() {
	pthread_rwlock_wrlock(&vpTreeLock);
	if (hamming_vptree) {
		delete hamming_vptree;
		delete edit_vptree;
		hamming_vptree = NULL;
		edit_vptree = NULL;
	}
	pthread_rwlock_unlock(&vpTreeLock);
}

ErrorCode VPTreeQueryAdd(QueryID query_id, const char* query_str, MatchType match_type, unsigned int match_dist) {
	clear_vptrees();
	Query* q = new Query(query_id, query_str, match_type, match_dist);
	pthread_mutex_lock(&queryMapLock);
	queryMap.insert(std::pair<QueryID, Query*>(query_id, q));
	pthread_mutex_unlock(&queryMapLock);
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
	pthread_mutex_lock(&queryMapLock);
	QueryMap::iterator found = queryMap.find(query_id);
	clear_vptrees();
	if (found == queryMap.end()) {
		pthread_mutex_unlock(&queryMapLock);
		return EC_SUCCESS;
	}
	Query* query = I2P(found->second);
	queryMap.erase(found);
	pthread_mutex_unlock(&queryMapLock);
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
	pthread_rwlock_rdlock(&wordMapLock);
	for(std::vector<std::string>::iterator i = y->begin();
		i != y->end();
		i++, j++) {
		Word* w = I2P(wordMap.find(*i)->second);

		x[j] = w->id();
	}
	pthread_rwlock_unlock(&wordMapLock);
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

		pthread_rwlock_rdlock(&wordMapLock);
		Word* word = I2P(wordMapByID.find(*i)->second);
		pthread_rwlock_unlock(&wordMapLock);
		for(std::set<QueryID>::iterator j = word->begin();
			j != word->end();
			j++) {

			bool match = true;

			QueryID query_id = *j;
			pthread_mutex_lock(&queryMapLock);
			Query* query = I2P(queryMap.find(query_id)->second);
			pthread_mutex_unlock(&queryMapLock);
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
ErrorCode VPTreeMatchDocument(DocID doc_id, const char* doc_str, std::vector<QueryID>& query_ids)
{
	new_vptrees_unless_exists();
	SET matchedHammingWords[TAU];
	SET matchedEditWords[TAU];
	std::set<std::string> docWords;

	// int old_perf_hamming = perf_counter_hamming;
	// int old_perf_edit = perf_counter_edit;
	ITERATE_QUERY_WORDS(doc_word, doc_str) {
		std::string doc_word_string = word_to_string(doc_word); // SPEED UP: question, I cannot reuse the pointer doc_str, but can I change *doc_str? */

		if (docWords.count(doc_word_string))
			continue;
		docWords.insert(doc_word_string);

		pthread_mutex_lock(&resultCacheLock);
		ResultCache::iterator found = resultCache.find(doc_word_string);
		ResultSet* rs;

		if (found == resultCache.end()) {
			pthread_mutex_unlock(&resultCacheLock);
			rs = new ResultSet();
			std::vector<std::string> results[TAU];

			pthread_rwlock_rdlock(&vpTreeLock);
			hamming_vptree->search(doc_word_string, TAU, results);
			pthread_rwlock_unlock(&vpTreeLock);

			for (int i = 0; i< TAU; i++) {
				rs->results_hamming[i] = do_union_y(&results[i]);
				results[i].clear();
			}

			pthread_rwlock_rdlock(&vpTreeLock);
			edit_vptree->search(doc_word_string, TAU, results);
			pthread_rwlock_unlock(&vpTreeLock);

			for (int i = 0; i< TAU; i++)
				rs->results_edit[i] = do_union_y(&results[i]);

			pthread_mutex_lock(&resultCacheLock);
			resultCache.insert(std::pair<std::string, ResultSet*>(doc_word_string, rs));
			pthread_mutex_unlock(&resultCacheLock);
		}
		else {
			rs = found->second;
			pthread_mutex_unlock(&resultCacheLock);
		}

		for (int i = 0; i < TAU; i++)
			do_union(&matchedHammingWords[i], &rs->results_hamming[i]);
		for (int i = 0; i < TAU; i++)
			do_union(&matchedEditWords[i], &rs->results_edit[i]);
	}
	// fprintf(stdout, "searching doc %d hamming/edit = %d/%d\n", doc_id, perf_counter_hamming - old_perf_hamming, perf_counter_edit - old_perf_edit);

	for (int i = 1; i < TAU; i++)
		do_union(&matchedHammingWords[i], &matchedHammingWords[i - 1]);

	for (int i = 1; i < TAU; i++)
		do_union(&matchedEditWords[i], &matchedEditWords[i - 1]);

	for (int i = 0; i < TAU; i++)
		do_union(&matchedEditWords[i], &matchedHammingWords[i]);

	words_to_queries(matchedHammingWords, matchedEditWords, query_ids);

	// performace: change iterator to const_iterator if possible.
	std::sort(query_ids.begin(), query_ids.end());

	return EC_SUCCESS;
}
