package com.bluesgao.edm.service;


import org.elasticsearch.action.search.SearchRequest;

import java.util.List;
import java.util.Map;

public interface EsOpsService {
    int bulkSave(String indexName, String idKey, List<Map<String, Object>> dataList);

    boolean indexIsExist(String indexName);

    public long getIndexDocCount(String indexName);

    List<Map<String, Object>> scroll(SearchRequest searchRequest);
}
