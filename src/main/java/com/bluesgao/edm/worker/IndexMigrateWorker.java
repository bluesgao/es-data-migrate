package com.bluesgao.edm.worker;

import com.alibaba.fastjson.JSON;
import com.bluesgao.edm.common.Result;
import com.bluesgao.edm.common.ResultCodeEnum;
import com.bluesgao.edm.condition.DataMigrateCondition;
import com.bluesgao.edm.condition.DateRangeDto;
import com.bluesgao.edm.condition.EsDataSyncConfigDto;
import com.bluesgao.edm.condition.SplitDateType;
import com.bluesgao.edm.service.CacheOpsService;
import com.bluesgao.edm.service.EsOpsService;
import com.bluesgao.edm.util.DateUtils;
import com.bluesgao.edm.util.OtherUtils;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.util.StringUtils;

import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

@Slf4j
public class IndexMigrateWorker implements Callable<Result<Long>> {

    private EsOpsService esOpsService;

    private CacheOpsService cacheOpsService;
    private DataMigrateCondition condition;

    public IndexMigrateWorker(EsOpsService esOpsService, CacheOpsService cacheOpsService, DataMigrateCondition condition) {
        this.esOpsService = esOpsService;
        this.cacheOpsService = cacheOpsService;
        this.condition = condition;
    }

    @Override
    public Result<Long> call() {
        Result<Long> syncResult = null;
        try {
            log.info("call doDataSync start condition:{}", JSON.toJSONString(condition));
            syncResult = doDataSync(condition);
            log.info("call doDataSync end condition:{},syncResult:{}", JSON.toJSONString(condition), JSON.toJSONString(syncResult));
            if (syncResult != null && syncResult.getData() != null && syncResult.getData() > 0) {
                //记录这天已经同步完成

                String redisKey = null;
                String field = null;
                Date date = DateUtils.dateParse(condition.getSplitCondition().getEnd(), DateUtils.DATE_TIME_PATTERN);
                //按日期分割
                if (condition.getSplitCondition().getSplitType().getCode().equals(SplitDateType.BY_DATE.getCode())) {
                    //key cf-sync-worker:jes:cf-content-5
                    redisKey = OtherUtils.genRedisKey(condition.getSourceIndex());
                    field = DateUtils.dateFormat(date, DateUtils.DATE_PATTERN);
                } else if (condition.getSplitCondition().getSplitType().getCode().equals(SplitDateType.BY_HOUR.getCode())) {
                    //按小时分割
                    //key cf-sync-worker:jes:cf-content-5:hour
                    redisKey = OtherUtils.genRedisKey(condition.getSourceIndex()) + ":hour";
                    field = DateUtils.dateFormat(date, DateUtils.DATE_HOUR_PATTERN);
                }

                if (redisKey != null && field != null) {
                    //key cf-sync-worker:jes:cf-content-5
                    //String redisKey = OtherUtils.genRedisKey(dataSyncConfigDto.getTo().getCluster(), dataSyncConfigDto.getTo().getIdx());

                    Long syncCount = syncResult.getData();
                    log.info("call doDataSync redisKey:{},syncCount:{}", redisKey, syncCount);
                    try {
                        log.info("call doDataSync redisKey:{},field:{},syncCount:{}", redisKey, field, syncCount);
                        boolean ret = cacheOpsService.hsetValue(redisKey, field, syncCount);
                        log.info("call doDataSync redisKey:{},field:{},syncCount:{},ret:{}", redisKey, field, syncCount, ret);
                    } catch (Exception e) {
                        log.error("call doDataSync redisKey:{},field:{},syncCount:{} error:{}", redisKey, field, syncCount, e);
                    }
                } else {
                    log.error("call doDataSync redis key is null");
                }
            }
        } catch (Exception e) {
            //e.printStackTrace();
            log.error("call doDataSync error:{}", e);
        }
        return syncResult;
    }


    private SearchRequest buildSearchRequest(DataMigrateCondition condition) {
        if ( condition.getSplitCondition().getDateField() != null) {
            String dateField = condition.getSplitCondition().getDateField();
            String startDateStr = condition.getSplitCondition().getStart();
            String endDateStr = condition.getSplitCondition().getEnd();

            Date startDate = null;
            Date endDate = null;
            if (!StringUtils.isEmpty(startDateStr)) {
                try {
                    startDate = DateUtils.dateParse(startDateStr, DateUtils.DATE_TIME_PATTERN);
                } catch (ParseException e) {
                    log.error("IndexMigrateWorker doDataSync startDateStr parse error:", e);
                }

            }
            if (!StringUtils.isEmpty(endDateStr)) {
                try {
                    endDate = DateUtils.dateParse(endDateStr, DateUtils.DATE_TIME_PATTERN);
                } catch (ParseException e) {
                    log.error("IndexMigrateWorker doDataSync endDateStr parse error:", e);
                }
            }

            if (endDate == null || startDate == null) {
                log.error("IndexMigrateWorker doDataSync endDate or startDate error condition:{}", JSON.toJSONString(condition));
                return null;
            }
            BoolQueryBuilder boolQueryBuilder = boolQuery();

            if (startDate != null) {
                boolQueryBuilder.filter(rangeQuery(dateField).gte(startDateStr));//大于等于开始时间
            }
            if (endDate != null) {
                boolQueryBuilder.filter(rangeQuery(dateField).lte(endDateStr));//小于等于结束时间
            }
            SearchRequest searchRequest = new SearchRequest(condition.getSourceIndex());
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            //sourceBuilder.size(100);
            sourceBuilder.query(boolQueryBuilder);
            log.info(sourceBuilder.toString());
            searchRequest.source(sourceBuilder);
        }
        return null;
    }

    private Result<Long> doDataSync(DataMigrateCondition condition) {
        log.info("IndexMigrateWorker doDataSync condition:{}", JSON.toJSONString(condition));

        String readIdx = condition.getSourceIndex();
        String writeIdx = condition.getDestinationIndex();
        String writeIdKey = condition.getIdKey();

        long totalCount = esOpsService.getIndexDocCount(readIdx);
        long syncCount = 0L;
        //todo 重写searchrequest
        //构造查询条件
        SearchRequest searchRequest = buildSearchRequest(condition);
        if (searchRequest == null) {
            return Result.genResult(ResultCodeEnum.PARAM_ERROR.getCode(), "buildSearchRequest error", null);
        }
        while (syncCount <= totalCount) {
            //读数据
            List<Map<String, Object>> res = esOpsService.scroll(searchRequest);
            if (res != null && res.size() > 0) {
                //写数据
                int temp = esOpsService.bulkSave(writeIdx, writeIdKey, res);
                if (temp > 0) {
                    syncCount = syncCount + temp;
                }
            }
        }
        log.info("IndexMigrateWorker doDataSync index:{},syncCount:{},start:{},end:{}", readIdx, JSON.toJSONString(syncCount), condition.getSplitCondition().getStart(), condition.getSplitCondition().getEnd());
        return Result.genResult(ResultCodeEnum.SUCCESS.getCode(), "index:" + readIdx + "syncCount:" + syncCount + ";[ " + condition.getSplitCondition().getStart() + " - " + condition.getSplitCondition().getEnd() + "]", syncCount);
    }
}
