package com.bluesgao.edm.worker;

import com.alibaba.fastjson.JSON;
import com.bluesgao.edm.common.Result;
import com.bluesgao.edm.common.ResultCodeEnum;
import com.bluesgao.edm.condition.DataMigrateCondition;
import com.bluesgao.edm.condition.SplitDateType;
import com.bluesgao.edm.service.CacheOpsService;
import com.bluesgao.edm.service.EsOpsService;
import com.bluesgao.edm.util.DateUtils;
import com.bluesgao.edm.util.OtherUtils;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

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
        Long syncCount = 0L;
        try {
            log.info("call doMigrate start condition:{}", JSON.toJSONString(condition));
            syncCount = esOpsService.migrate(condition);
            log.info("call doMigrate end condition:{},syncCount:{}", JSON.toJSONString(condition), syncCount);
            if (syncCount > 0) {
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
                    log.info("call doMigrate redisKey:{},syncCount:{}", redisKey, syncCount);
                    try {
                        log.info("call doMigrate redisKey:{},field:{},syncCount:{}", redisKey, field, syncCount);
                        boolean ret = cacheOpsService.hsetValue(redisKey, field, syncCount);
                        log.info("call doMigrate redisKey:{},field:{},syncCount:{},ret:{}", redisKey, field, syncCount, ret);
                    } catch (Exception e) {
                        log.error("call doMigrate redisKey:{},field:{},syncCount:{} error:{}", redisKey, field, syncCount, e);
                    }
                } else {
                    log.error("call doMigrate redis key is null");
                }
            }
        } catch (Exception e) {
            //e.printStackTrace();
            log.error("call doMigrate error:{}", e);
        }
        return Result.genResult(ResultCodeEnum.SUCCESS.getCode(), "", syncCount);
    }


}
