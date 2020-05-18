package com.bluesgao.edm.service.impl;

import com.alibaba.fastjson.JSON;
import com.bluesgao.edm.common.Result;
import com.bluesgao.edm.condition.DataMigrateCondition;
import com.bluesgao.edm.condition.DateRangeDto;
import com.bluesgao.edm.condition.SplitCondition;
import com.bluesgao.edm.condition.SplitDateType;
import com.bluesgao.edm.service.CacheOpsService;
import com.bluesgao.edm.service.DataMigrateService;
import com.bluesgao.edm.service.EsOpsService;
import com.bluesgao.edm.util.DateUtils;
import com.bluesgao.edm.util.OtherUtils;
import com.bluesgao.edm.worker.IndexMigrateWorker;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.util.*;
import java.util.concurrent.*;

@Service
@Slf4j
public class DataMigrateServiceImpl implements DataMigrateService {
    @Autowired
    private ThreadPoolTaskExecutor taskExecutor;

    @Autowired
    private EsOpsService esOpsService;

    @Autowired
    private CacheOpsService cacheOpsService;
    public Result indexMigrate(DataMigrateCondition condition) {
        //条件检查
        if (!doCheck(condition)){
            log.error("indexMigrate doCheck error:{}", JSON.toJSONString(condition));
            return null;
        }

        //条件拆分
        Map<String, SplitCondition> splitConditionMap = doSplit(condition.getSplitCondition());
        if (splitConditionMap==null || splitConditionMap.size()==0){
            log.error("indexMigrate splitDateMap error:{}", JSON.toJSONString(condition));
            return null;
        }

        //任务拆分
        //1.定义CompletionService
        CompletionService<Result<Long>> completionService = new ExecutorCompletionService(taskExecutor);
        //结果集
        List<Result<Long>> resultList = new ArrayList<Result<Long>>();
        //2,向线程池提交任务
        int taskCount = 0;
        for (String key : splitConditionMap.keySet()) {
            String redisKey = OtherUtils.genRedisKey(condition.getSourceIndex()) + ":hour";
            //去除已经同步过的日期
            String ret = cacheOpsService.hgetValue(redisKey, key).toString();
            log.debug("redis hget redisKey:{},field:{},ret:{}", redisKey, key, ret);
            if (ret != null && Integer.valueOf(ret) > 0) {
                log.info("当前日期{}已经同步,跳过 redis hget redisKey:{},field:{},ret:{}", key, redisKey, key, ret);
                continue;
            }
            DataMigrateCondition tempCondition = new DataMigrateCondition();
            BeanUtils.copyProperties(condition, tempCondition);
            tempCondition.setSplitCondition(splitConditionMap.get(key));

            log.info("有效任务key:{},tempCondition:{}", key, JSON.toJSONString(tempCondition));
            IndexMigrateWorker IndexMigrateWorker = new IndexMigrateWorker(esOpsService, cacheOpsService, tempCondition);
            completionService.submit(IndexMigrateWorker);
            taskCount++;
        }

        //3.获取线程执行结果，使用内部阻塞队列的take()
        for (int i = 0; i < taskCount; i++) {
            //采用completionService.take()，内部维护阻塞队列，任务先完成的先获取到
            Result<Long> result = null;
            try {
                //todo timeout 有堵塞风险 线程执行时间与数据量大小正相关，不好评估
                result = completionService.take().get(30, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                //e.printStackTrace();
                log.error("worker线程被中断：{}", e);
            } catch (ExecutionException e) {
                //e.printStackTrace();
                log.error("worker线程执行异常：{}", e);
            } catch (TimeoutException e) {
                //e.printStackTrace();
                log.error("worker线程获取结果超时：{}", e);
            }
            log.info("indexMigrate call result:{}", JSON.toJSONString(result));
            resultList.add(result);
        }
        log.info("indexMigrate call all result:{}", JSON.toJSONString(resultList));
        return null;
    }

    private boolean doCheck(DataMigrateCondition condition){
        if (condition==null
                || condition.getSourceIndex()==null
                || condition.getSourceIndex().length()==0
                || condition.getDestinationIndex()==null
                || condition.getDestinationIndex().length()==0
                || condition.getSplitCondition().getDateField()==null
                || condition.getSplitCondition().getDateField().length()==0
                || condition.getSplitCondition().getStart()==null
                ||condition.getSplitCondition().getStart().length()==0
                || condition.getSplitCondition().getEnd()==null
                || condition.getSplitCondition().getEnd().length()==0){
            log.error("indexMigrate doCheck error:{}", JSON.toJSONString(condition));
            return false;
        }

        Date start = null;
        Date end = null;
        try {
            start = DateUtils.dateParse(condition.getSplitCondition().getStart(), DateUtils.DATE_PATTERN);
            end = DateUtils.dateParse(condition.getSplitCondition().getEnd(), DateUtils.DATE_PATTERN);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        if (start==null || end==null){
            log.error("indexMigrate doCheck start or end is error:{}", JSON.toJSONString(condition));
            return false;
        }

        if (!esOpsService.indexIsExist(condition.getSourceIndex())){
            log.error("indexMigrate doCheck SourceIndex is not exist:{}", condition.getSourceIndex());
            return false;
        }

        if (!esOpsService.indexIsExist(condition.getDestinationIndex())){
            log.error("indexMigrate doCheck DestinationIndex is not exist:{}", condition.getDestinationIndex());
            return false;
        }

        if (esOpsService.getIndexDocCount(condition.getSourceIndex())<=0){
            log.error("indexMigrate doCheck SourceIndex doc count is zero:{}", condition.getSourceIndex());
            return false;
        }

        return true;
    }

    private Map<String, SplitCondition> doSplit(SplitCondition condition){
        Date start = null;
        Date end = null;
        try {
            start = DateUtils.dateParse(condition.getStart(), DateUtils.DATE_PATTERN);
            end = DateUtils.dateParse(condition.getEnd(), DateUtils.DATE_PATTERN);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return splitRangeDateByType(start, end, condition.getDateField(), condition.getSplitType());
    }

    private Map<String, SplitCondition> splitRangeDateByType(Date startDate, Date endDate, String dateField, SplitDateType type) {
        Map<String, SplitCondition> resultMap = new HashMap<>();
        int between = 0;
        try {
            between = DateUtils.dateBetween(startDate, endDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        for (int i = 0; i <= between; i++) {
            Date tempDate = DateUtils.dateAddDays(startDate, i);
            String yyyyMMddStr = null;
            try {
                yyyyMMddStr = DateUtils.dateFormat(tempDate, DateUtils.DATE_PATTERN);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            if (yyyyMMddStr != null) {
                if (type.getCode().equals(SplitDateType.BY_DATE.getCode())) {
                    SplitCondition splitCondition = new SplitCondition();
                    splitCondition.setStart(yyyyMMddStr + DateUtils.DAY_BEGIN_TIME);
                    splitCondition.setEnd(yyyyMMddStr + DateUtils.DAY_END_TIME);
                    splitCondition.setDateField(dateField);
                    splitCondition.setSplitType(SplitDateType.BY_DATE);
                    resultMap.put(yyyyMMddStr, splitCondition);
                } else if (type.getCode().equals(SplitDateType.BY_HOUR.getCode())) {
                    List<Map<Date, Date>> hoursMapList = DateUtils.splitDateByHours(tempDate);
                    for (Map<Date, Date> hourMap : hoursMapList) {
                        for (Date key : hourMap.keySet()) {
                            try {
                                String yyyyMMddHHStr = DateUtils.dateFormat(key, DateUtils.DATE_HOUR_PATTERN);
                                SplitCondition splitCondition = new SplitCondition();
                                splitCondition.setStart(DateUtils.dateFormat(key, DateUtils.DATE_TIME_PATTERN));
                                splitCondition.setEnd(DateUtils.dateFormat(hourMap.get(key), DateUtils.DATE_TIME_PATTERN));
                                splitCondition.setDateField(dateField);
                                splitCondition.setSplitType(SplitDateType.BY_HOUR);
                                if (splitCondition != null && yyyyMMddHHStr != null && splitCondition.getStart() != null && splitCondition.getEnd() != null) {
                                    resultMap.put(yyyyMMddHHStr, splitCondition);
                                }
                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                        }

                    }
                }
            }
        }
        return resultMap;
    }

}
