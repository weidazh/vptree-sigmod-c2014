#ifndef __VPTREE_MATCH_H_
#define __VPTREE_MATCH_H_

#include "core.h"
#include <vector>

ErrorCode VPTreeQueryAdd(QueryID query_id, const char* query_str, MatchType match_type, unsigned int match_dist);
ErrorCode VPTreeQueryRemove(QueryID query_id);
// ErrorCode VPTreeMatchDocument(DocID doc_id, const char* doc_str);
ErrorCode VPTreeMatchDocument(DocID doc_id, const char* doc_str, std::vector<QueryID>& query_ids);

void vptree_system_init();
void vptree_doc_worker_init();
void vptree_doc_worker_destroy();
void vptree_system_destroy();
#endif
