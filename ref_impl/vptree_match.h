#ifndef __VPTREE_MATCH_H_
#define __VPTREE_MATCH_H_

#include "core.h"
#include <vector>

ErrorCode VPTreeQueryAdd(QueryID query_id, const char* query_str, MatchType match_type, unsigned int match_dist);
ErrorCode VPTreeQueryRemove(QueryID query_id);
// ErrorCode VPTreeMatchDocument(DocID doc_id, const char* doc_str);
ErrorCode VPTreeMatchDocument(DocID doc_id, const char* doc_str, std::vector<QueryID>& query_ids);
void BuildIndexPre();
void BuildIndexThread();
void BuildIndexPost();
ErrorCode VPTreeMasterMatchDocument(DocID doc_id, const char* doc_str);
ErrorCode VPTreeWordFeeder(int word_feeder_id);
void WaitWordResults(int waiter_id);
void MasterMergeCache();

void vptree_system_init();
void vptree_doc_worker_init();
void vptree_doc_worker_destroy();
void vptree_system_destroy();
#endif
