package com.bluesgao.edm.service;


import com.bluesgao.edm.condition.DataMigrateCondition;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.List;
import java.util.Map;

public interface EsOpsService {
    int bulkSave(String indexName, String idKey, List<Map<String, Object>> dataList);

    boolean indexIsExist(String indexName);

    long getIndexDocCount(String indexName, QueryBuilder queryBuilder);

    long migrate(DataMigrateCondition condition);
}
