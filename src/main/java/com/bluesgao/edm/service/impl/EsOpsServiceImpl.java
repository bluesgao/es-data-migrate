package com.bluesgao.edm.service.impl;

import com.alibaba.fastjson.JSON;
import com.bluesgao.edm.condition.DataMigrateCondition;
import com.bluesgao.edm.service.EsOpsService;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

/**
 * ES底层操作方法
 */
@Slf4j
@Service
public class EsOpsServiceImpl implements EsOpsService {

    @Resource
    private RestHighLevelClient restHighLevelClient;

    private int threshold = 5;

    @Override
    public long getIndexDocCount(String indexName, QueryBuilder queryBuilder) {
        try {
            String[] idxs = {indexName};
            CountRequest request = new CountRequest(idxs);
            if (queryBuilder != null) {
                request.query(queryBuilder);
            }
            CountResponse response = restHighLevelClient.count(request, RequestOptions.DEFAULT);
            log.info("getIndexDocCount response:{}", JSON.toJSONString(response));
            return response.getCount();
        } catch (Exception e) {
            log.error("indexIsExist error:{}", e);
        }
        return 0L;
    }

    public long migrate(DataMigrateCondition condition) {
        log.info("EsOpsServiceImpl migrate condition:{}", JSON.toJSONString(condition));

        //构造查询条件
        QueryBuilder queryBuilder = genQueryBuilder(condition);

        long syncCount = 0L;
        SearchRequest searchRequest = genSearchRequest(condition, queryBuilder);
        if (searchRequest == null) {
            return 0L;
        }
        try {
            Scroll scroll = new Scroll(TimeValue.timeValueMillis(1L));//设定滚动时间间隔
            searchRequest.scroll(scroll);
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            String scrollId = searchResponse.getScrollId();
            SearchHit[] hits = searchResponse.getHits().getHits();
            while (hits != null && hits.length > 0) {
                List<Map<String, Object>> dataList = new ArrayList<>();
                for (SearchHit hit : hits) {
                    dataList.add(hit.getSourceAsMap());
                }
                //写
                int writeCount = bulkSave(condition.getDestinationIndex(), condition.getIdKey(), dataList);
                syncCount = syncCount + writeCount;

                SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
                searchScrollRequest.scroll(scroll);
                SearchResponse searchScrollResponse = restHighLevelClient.scroll(searchScrollRequest, RequestOptions.DEFAULT);
                scrollId = searchScrollResponse.getScrollId();
                hits = searchScrollResponse.getHits().getHits();
            }
            //及时清除es快照，释放资源
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            restHighLevelClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return syncCount;
    }

    private QueryBuilder genQueryBuilder(DataMigrateCondition condition) {
        String dateField = condition.getSplitCondition().getDateField();
        String startDateStr = condition.getSplitCondition().getStart();
        String endDateStr = condition.getSplitCondition().getEnd();
        BoolQueryBuilder boolQueryBuilder = boolQuery();
        boolQueryBuilder.filter(rangeQuery(dateField).gte(startDateStr).lte(endDateStr));
        return boolQueryBuilder;
    }

    private SearchRequest genSearchRequest(DataMigrateCondition condition, QueryBuilder queryBuilder) {
        SearchRequest searchRequest = new SearchRequest(condition.getSourceIndex());
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(threshold);
        sourceBuilder.query(queryBuilder);
        log.info(sourceBuilder.toString());
        searchRequest.source(sourceBuilder);
        return searchRequest;
    }

    @Override
    public boolean indexIsExist(String indexName) {
        log.info("indexIsExist indexName:{}", indexName);
        boolean exists = false;
        try {
            GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
            getIndexRequest.humanReadable(true);
            exists = restHighLevelClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return exists;
    }

    @Override
    public int bulkSave(String indexName, String idKey, List<Map<String, Object>> bulks) {
        log.debug("bulkSave start dataList.size:{},indexName:{},idKey:", bulks.size(), indexName, idKey);
        try {
            BulkRequest bulkRequest = new BulkRequest();
            for (Map<String, Object> bulk : bulks) {
                IndexRequest request = new IndexRequest("post");
                request.index(indexName).id(String.valueOf(bulk.get(idKey))).source(JSON.toJSONString(bulk), XContentType.JSON);
                bulkRequest.add(request);
            }
            BulkResponse response = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            log.error(" bulkSave bulkResponse:{}，FailureMessage:", JSON.toJSONString(response), response.buildFailureMessage());
            if (!response.hasFailures()) {
                return bulks.size();
            }
        } catch (Exception e) {
            log.error("bulkSave error:{}", e);
        }
        return 0;
    }
}
