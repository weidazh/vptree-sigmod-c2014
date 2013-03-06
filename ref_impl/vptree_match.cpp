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

// must be int
int hamming(const std::string& a, const std::string& b) {
	unsigned int oo = 0x7FFFFFFF;
	unsigned int dist = HammingDistance(a.c_str(), a.length(), b.c_str(), b.length());
	if (dist == oo) {
		return oo - TAU;
	}
	return dist;
}

int edit(const std::string& a, const std::string& b) {
	unsigned int oo = 0x7FFFFFFF;
	unsigned int dist = EditDistance(a.c_str(), a.length(), b.c_str(), b.length());
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

static int max_word_id = 0;

class Word {
public:
	std::string word;
	std::set<QueryID> hamming_queries; // PERFORMANCE could be only a counter!
	std::set<QueryID> edit_queries; // PERFORMANCE could be only a counter!
	std::set<QueryID> first_word_queries;
	int word_id;

	Word(std::string word)
		: word(word), hamming_queries(), edit_queries(), first_word_queries() {

		word_id = max_word_id;
		max_word_id += 1;
	}
	void push_query(QueryID q, bool first, MatchType match_type) {
		if (match_type == MT_HAMMING_DIST || match_type == MT_EXACT_MATCH)
			this->hamming_queries.insert(q);
		if (match_type == MT_EDIT_DIST)
			this->edit_queries.insert(q);
		if (first)
			this->first_word_queries.insert(q);
	}
	void remove_query(QueryID q, MatchType match_type) {
		if (match_type == MT_HAMMING_DIST || match_type == MT_EXACT_MATCH)
			this->hamming_queries.erase(q);
		if (match_type == MT_EDIT_DIST)
			this->edit_queries.erase(q);
	}
	void remove_first_word_query(QueryID q) {
		this->first_word_queries.erase(q);
	}
	int id() {
		return word_id;
	}
	bool empty() {
		return this->hamming_queries.empty() && this->edit_queries.empty();
	}
	bool hasHamming() {
		return ! this->hamming_queries.empty();
	}
	bool hasEdit() {
		return ! this->edit_queries.empty();
	}
};

typedef std::map<std::string, Word&> WordMap;
typedef std::map<int, Word&> WordMapByID;
WordMap wordMap;
WordMapByID wordMapByID;
// std::set<std::string> wordSet;
typedef VpTree<std::string, int, hamming> HammingVpTree;
typedef VpTree<std::string, int, edit> EditVpTree;
HammingVpTree* hamming_vptree;
EditVpTree* edit_vptree;
typedef std::map<QueryID, Query&> QueryMap;
QueryMap queryMap;

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

static void new_vptrees_unless_exists() {
	std::vector<std::string> hammingWordList;
	std::vector<std::string> editWordList;
	if (! hamming_vptree) {
		hamming_vptree = new HammingVpTree();
		edit_vptree = new EditVpTree();
		for(std::map<std::string, Word&>::iterator i = wordMap.begin();
			i != wordMap.end(); i++) {

			if (i->second.hasHamming())
				hammingWordList.push_back(i->first);
			if (i->second.hasEdit())
				editWordList.push_back(i->first);
		}
		hamming_vptree->create(hammingWordList);
		edit_vptree->create(editWordList);
	}
}

static void clear_vptrees() {
	if (hamming_vptree) {
		delete hamming_vptree;
		delete edit_vptree;
		hamming_vptree = NULL;
		edit_vptree = NULL;
	}
}

ErrorCode VPTreeQueryAdd(QueryID query_id, const char* query_str, MatchType match_type, unsigned int match_dist) {
	clear_vptrees();
	Query* q = new Query(query_id, query_str, match_type, match_dist);
	queryMap.insert(std::pair<QueryID, Query&>(query_id, *q));
	bool first = true;
	int i = 0;
	ITERATE_QUERY_WORDS(query_word, query_str) {
		std::string query_word_string = word_to_string(query_word);
		WordMap::iterator found = wordMap.find(query_word_string);
		Word* word;
		if (found != wordMap.end()) {
			word = &(found->second);
			word->push_query(query_id, first, match_type);
		}
		else {
			word = new Word(query_word_string);
			word->push_query(query_id, first, match_type);
			wordMap.insert(std::pair<std::string, Word&>(query_word_string, *word));
			wordMapByID.insert(std::pair<int, Word&>(word->id(), *word));
			// wordSet.insert(query_word_string);
		}
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
	QueryMap::iterator found = queryMap.find(query_id);
	clear_vptrees();
	if (found == queryMap.end()) {
		return EC_SUCCESS;
	}
	Query& query = found->second;
	queryMap.erase(found);
	bool first = true;
	ITERATE_QUERY_WORDS(query_word, query.getQueryStr()) {
		std::string query_word_string = word_to_string(query_word);
		WordMap::iterator word_found = wordMap.find(query_word_string);
		if (word_found == wordMap.end()) {
			fprintf(stderr, "ERROR: word not found for query(%s) and word(%s)\n", query.getQueryStr(), query_word_string.c_str());
			continue;
		}
		Word* word = &(word_found->second);
		word->remove_query(query_id, query.match_type);
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
		first = false;
	}
	delete &query;

	return EC_SUCCESS;
}

static void do_union_x(std::set<int> * x, std::set<std::string> * y) {
	for(typename std::set<std::string>::iterator i = y->begin();
		i != y->end();
		i++) {

		x->insert(wordMap.find(*i)->second.id());
	}
}

template <typename T>
static void do_union(std::set<T> * x, std::set<T> * y) {
	for(typename std::set<T>::iterator i = y->begin();
		i != y->end();
		i++) {

		x->insert(*i);
	}
}

ErrorCode VPTreeMatchDocument(DocID doc_id, const char* doc_str, std::vector<QueryID>& query_ids)
{
	new_vptrees_unless_exists();
	std::set<int> matchedHammingWords[4];
	std::set<int> matchedEditWords[4];
	std::set<std::string> docWords;

	ITERATE_QUERY_WORDS(doc_word, doc_str) {
		std::string doc_word_string = word_to_string(doc_word); // SPEED UP: question, I cannot reuse the pointer doc_str, but can I change *doc_str? */

		if (docWords.count(doc_word_string))
			continue;
		docWords.insert(doc_word_string);

		{
			std::set<std::string> results[4];

			hamming_vptree->search(doc_word_string, 4, results);

			do_union_x(&matchedHammingWords[0], &results[0]);
			do_union_x(&matchedHammingWords[1], &results[1]);
			do_union_x(&matchedHammingWords[2], &results[2]);
			do_union_x(&matchedHammingWords[3], &results[3]);
		}

		{
			std::set<std::string> results[4];

			edit_vptree->search(doc_word_string, 4, results);

			do_union_x(&matchedEditWords[0], &results[0]);
			do_union_x(&matchedEditWords[1], &results[1]);
			do_union_x(&matchedEditWords[2], &results[2]);
			do_union_x(&matchedEditWords[3], &results[3]);
		}
	}

	do_union(&matchedHammingWords[1], &matchedHammingWords[0]);
	do_union(&matchedHammingWords[2], &matchedHammingWords[1]);
	do_union(&matchedHammingWords[3], &matchedHammingWords[2]);

	do_union(&matchedEditWords[0], &matchedHammingWords[0]);
	do_union(&matchedEditWords[1], &matchedEditWords[0]);
	do_union(&matchedEditWords[1], &matchedHammingWords[1]);
	do_union(&matchedEditWords[2], &matchedEditWords[1]);
	do_union(&matchedEditWords[2], &matchedHammingWords[2]);
	do_union(&matchedEditWords[3], &matchedEditWords[2]);
	do_union(&matchedEditWords[3], &matchedHammingWords[3]);

	for(std::set<int>::iterator i = matchedEditWords[3].begin();
		i != matchedEditWords[3].end();
		i++) {

		Word& word = wordMapByID.find(*i)->second;
		for(std::set<QueryID>::iterator j = word.first_word_queries.begin();
			j != word.first_word_queries.end();
			j++) {

			bool match = true;

			QueryID query_id = *j;
			Query& query = queryMap.find(query_id)->second;
			for (int j = 0; j < MAX_QUERY_WORDS && query.word_ids[j] != -1; j++) {
				int id = query.word_ids[j];
				// if query is hamming
				if ((query.match_type == MT_EXACT_MATCH &&
					! matchedHammingWords[0].count(id)) ||
				    (query.match_type == MT_HAMMING_DIST &&
					! matchedHammingWords[query.match_dist].count(id)) ||
				    (query.match_type == MT_EDIT_DIST &&
					! matchedEditWords[query.match_dist].count(id))) {

					match = false;
					break;
				}
			}

			if (match) {
				query_ids.push_back(query_id);
			}
		}
	}

	// performace: change iterator to const_iterator if possible.
	std::sort(query_ids.begin(), query_ids.end());

	return EC_SUCCESS;
}
