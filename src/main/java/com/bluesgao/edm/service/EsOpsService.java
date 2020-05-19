package com.bluesgao.edm.service;


import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.List;
import java.util.Map;

public interface EsOpsService {
    int bulkSave(String indexName, String idKey, List<Map<String, Object>> dataList);

    boolean indexIsExist(String indexName);

    public long getIndexDocCount(String indexName, QueryBuilder queryBuilder);

    List<Map<String, Object>> scroll(SearchRequest searchRequest);
}
