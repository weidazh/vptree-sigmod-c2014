#include "vptree_match.h"
#include "vptree.h"
#include <cstdio>
#include <string>
#include <cstring>
#include <exception>
#include <vector>
#include <set>
#include <map>

#define NON_NULL(a) ((*(a)) && ((*(a)) != ' '))

class myexcp: public std::exception {
	public:
	char str[100];
	myexcp(const char* str) {
		strncpy(this->str, str, 100);
		this->str[99] = 0;
	}
};

int hamming(const char* a, const char* b) {
	const int oo=0x7FFFFFFF;
	unsigned int num_mismatches=0;

	while(NON_NULL(a) && NON_NULL(b)) {
		if (*a != *b)
			num_mismatches++;
		a ++;
		b ++;
	}

	if (NON_NULL(a) || NON_NULL(b))
		return oo;

	return num_mismatches;
}

int hamming2(const std::string& a, const std::string& b) {
	return 0;
}

#define VALIDATE_LENGTH
class Query {
private:
	QueryID query_id;
	char query_str[MAX_QUERY_LENGTH];
	MatchType match_type;
	unsigned int match_dist;

public:
	Query(QueryID query_id, const char* query_str, MatchType match_type, unsigned int match_dist) {
		this->query_id = query_id;
#ifdef VALIDATE_LENGTH
		if (strlen(this->query_str) >= MAX_QUERY_LENGTH){
			throw new myexcp("query string >= MAX_QUERY_LENGTH");
		}
#endif
		strcpy(this->query_str, query_str);
		this->match_type = match_type;
		this->match_dist = match_dist;
	}

	const char* getQueryStr() {
		return query_str;
	}
};

class Word {
private:
	std::string word;
	std::set<QueryID> queries; // PERFORMANCE could be only a counter!
public:
	std::set<QueryID> first_word_queries;

	Word(std::string word)
		: queries(), first_word_queries() {
		this->word = word;
	}
	void push_query(QueryID q, bool first) {
		this->queries.insert(q);
		if (first)
			this->first_word_queries.insert(q);
	}
	void remove_query(QueryID q) {
		this->queries.erase(q);
	}
	void remove_first_word_query(QueryID q) {
		this->first_word_queries.erase(q);
	}
	bool empty() {
		return this->queries.empty();
	}
};

typedef std::map<std::string, Word> WordMap;
WordMap wordMap;
std::set<std::string> wordSet;
typedef VpTree<std::string, int, hamming2> HammingVpTree;
HammingVpTree* vptree;
typedef std::map<QueryID, Query> QueryMap;
QueryMap queryMap;

const char* next_word_in_query(const char* query_str) {
	while (NON_NULL(query_str))
		query_str ++;
	while (*query_str == ' ')
		query_str ++;
	return query_str;
}

std::string word_to_string(const char* word) {
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

#define ITERATE_QUERY_WORDS(key, begin) for (const char* key = begin; *key; key = next_word_in_query(key))

ErrorCode VPTreeQueryAdd(QueryID query_id, const char* query_str, MatchType match_type, unsigned int match_dist) {
	if (match_type == MT_HAMMING_DIST) {
		Query* q = new Query(query_id, query_str, match_type, match_dist);
		queryMap.insert(std::pair<QueryID, Query>(query_id, *q));
		bool first = true;
		ITERATE_QUERY_WORDS(query_word, query_str) {
			std::string query_word_string = word_to_string(query_word);
			WordMap::iterator found = wordMap.find(query_str);
			if (found != wordMap.end()) {
				found->second.push_query(query_id, first);
			}
			else {
				Word* word = new Word(query_word);
				word->push_query(query_id, first);
				wordMap.insert(std::pair<std::string, Word>(query_word_string, *word));
				wordSet.insert(query_word_string);
			}
			first = false;
		}
	}
	return EC_SUCCESS;
}

ErrorCode VPTreeQueryRemove(QueryID query_id) {
	QueryMap::iterator found = queryMap.find(query_id);
	queryMap.erase(found);
	bool first = true;
	ITERATE_QUERY_WORDS(query_word, found->second.getQueryStr()) {
		std::string query_word_string = word_to_string(query_word);
		WordMap::iterator word_found = wordMap.find(query_word_string);
		Word& word = word_found->second;
		word.remove_query(query_id);
		if (first)
			word.remove_first_word_query(query_id);
		if (word.empty()) {
			// dispose or delay disposing!
			wordSet.erase(query_word_string);
			wordMap.erase(word_found);
		}
		first = false;
	}
	return EC_SUCCESS;
}

ErrorCode VPTreeMatchDocument(DocID doc_id, const char* doc_str, std::vector<QueryID> query_ids)
{
	if (! vptree) {
		vptree = new HammingVpTree();
		std::vector<std::string> wordList;
		for(std::set<std::string>::iterator i = wordSet.begin();
			i != wordSet.end(); i++) {

			wordList.push_back(*i);
		}
		vptree->create(wordList);
	}

	std::set<std::string> matchedWords;
	std::set<QueryID> matchedQueries;

	ITERATE_QUERY_WORDS(doc_word, doc_str) {
		std::string doc_word_string = word_to_string(doc_word); // SPEED UP: question, I cannot reuse the pointer doc_str, but can I change *doc_str? */
		std::vector<std::string> results;
		std::vector<int> distances;

		vptree->search(doc_word_string, 100, &results, &distances);
		for(std::vector<std::string>::iterator j = results.begin(); j != results.end(); j++) {
			matchedWords.insert(*j);
		}
	}

	for(std::set<std::string>::iterator i = matchedWords.begin();
		i != matchedWords.end();
		i++) {

		Word& word = wordMap.find(*i)->second;
		for(std::set<QueryID>::iterator j = word.first_word_queries.begin();
			j != word.first_word_queries.end();
			j++) {

			bool match = true;

			QueryID query_id = *j;
			Query& query = queryMap.find(query_id)->second;
			ITERATE_QUERY_WORDS(query_word, query.getQueryStr()) {
				std::string query_word_string = word_to_string(query_word);
				if (query_word_string != *i || ! matchedWords.count(query_word_string)) {
					match = false;
					break;
				}
			}

			if (match) {
				matchedQueries.insert(query_id);
			}

		}
	}
	// performace: change iterator to const_iterator if possible.

	bool correct = true;
	if (matchedQueries.size() != query_ids.size()) {
		fprintf(stderr, "Error, incorrect\n");
		correct = false;
	}

	for(std::vector<QueryID>::iterator i = query_ids.begin();
		i != query_ids.end();
		i++) {

		if (! matchedQueries.count(*i)) {
			correct = false;
		}
	}

	if (!correct) {
		fprintf(stderr, "\n");
		fprintf(stderr, "correct: ");
		for (std::vector<QueryID>::iterator i = query_ids.begin();
			i != query_ids.end();
			i++ )
		{
			fprintf(stderr, "%d ", *i);
		}
		fprintf(stderr, "\n");
		fprintf(stderr, "my resu: ");
		for (std::set<QueryID>::iterator i = matchedQueries.begin();
			i != matchedQueries.end();
			i++ )
		{
			fprintf(stderr, "%d ", *i);
		}
		fprintf(stderr, "\n");
	}

	// cmpare query_ids and matchedQueries

	return EC_SUCCESS;
}
