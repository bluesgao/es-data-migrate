package com.bluesgao.edm.service.impl;

import com.alibaba.fastjson.JSON;
import com.bluesgao.edm.common.Result;
import com.bluesgao.edm.common.ResultCodeEnum;
import com.bluesgao.edm.conf.DateRangeDto;
import com.bluesgao.edm.conf.EsDataSyncConfigDto;
import com.bluesgao.edm.conf.SplitDateType;
import com.bluesgao.edm.service.*;
import com.bluesgao.edm.util.DateUtils;
import com.bluesgao.edm.util.OtherUtils;
import com.bluesgao.edm.worker.Es2EsWorker;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

@Service
@Slf4j
public class DataSyncServiceImpl implements DataSyncService {

    @Autowired
    private ThreadPoolTaskExecutor taskExecutor;

    @Autowired
    SplitStrategy splitStrategy;

    @Autowired
    private EsOpsService esOpsService;

    @Autowired
    private CacheOpsService cacheOpsService;

    @Override
    public Result index2index(EsDataSyncConfigDto dataSyncConfigDto) {
        log.info("index2index conf:{}", JSON.toJSONString(dataSyncConfigDto));

        Result checkResult = check(dataSyncConfigDto);
        log.debug("index2index checkResult:{}", JSON.toJSONString(checkResult));
        if (checkResult != null) {
            return checkResult;
        }

        Date start = null;
        Date end = null;
        try {
            start = DateUtils.dateParse(dataSyncConfigDto.getDateRangeDto().getStart(), DateUtils.DATE_PATTERN);
            end = DateUtils.dateParse(dataSyncConfigDto.getDateRangeDto().getEnd(), DateUtils.DATE_PATTERN);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        String dateField = dataSyncConfigDto.getDateRangeDto().getDateField();
        String redisKey = null;

        Map<String, DateRangeDto> splitDateMap = null;
        //按日期分割
        if (dataSyncConfigDto.getDateRangeDto().getSplitType().getCode().equals(SplitDateType.BY_DATE.getCode())) {
            splitDateMap = splitStrategy.splitRangeDateByType(start, end, dateField, SplitDateType.BY_DATE);
            //key cf-sync-worker:jes:cf-content-5
            redisKey = OtherUtils.genRedisKey(dataSyncConfigDto.getTo().getCluster(), dataSyncConfigDto.getTo().getIdx());
        } else if (dataSyncConfigDto.getDateRangeDto().getSplitType().getCode().equals(SplitDateType.BY_HOUR.getCode())) {
            //按小时分割
            splitDateMap = splitStrategy.splitRangeDateByType(start, end, dateField, SplitDateType.BY_HOUR);
            //key cf-sync-worker:jes:cf-content-5:hour
            redisKey = OtherUtils.genRedisKey(dataSyncConfigDto.getTo().getCluster(), dataSyncConfigDto.getTo().getIdx()) + ":hour";
        }
        log.info("index2index splitDateMap.size:{},splitDateMap:{}", splitDateMap.size(), JSON.toJSONString(splitDateMap));

        //结果集
        List<Result<Long>> resultList = new ArrayList<Result<Long>>();

        if (splitDateMap != null && splitDateMap.size() > 0) {

            //1.定义CompletionService
            CompletionService<Result<Long>> completionService = new ExecutorCompletionService(taskExecutor);

            //2,向线程池提交任务
            int taskCount = 0;
            for (String key : splitDateMap.keySet()) {

                //去除已经同步过的日期
                String ret = cacheOpsService.hgetValue(redisKey, key).toString();
                log.debug("redis hget redisKey:{},field:{},ret:{}", redisKey, key, ret);
                if (ret != null && Integer.valueOf(ret) > 0) {
                    log.info("当前日期{}已经同步,跳过 redis hget redisKey:{},field:{},ret:{}", key, redisKey, key, ret);
                    continue;
                }
                //dataSyncConfigDto.setDateRangeDto(splitDateMap.get(key));
                EsDataSyncConfigDto tempConfig = new EsDataSyncConfigDto();
                BeanUtils.copyProperties(dataSyncConfigDto, tempConfig);
                tempConfig.setDateRangeDto(splitDateMap.get(key));
                log.info("有效任务key:{},tempConfig:{}", key, JSON.toJSONString(tempConfig));
                Es2EsWorker es2EsWorker = new Es2EsWorker(esOpsService, cacheOpsService, tempConfig);
                completionService.submit(es2EsWorker);
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
                log.info("index2index call result:{}", JSON.toJSONString(result));
                resultList.add(result);
            }
            log.info("index2index call all result:{}", JSON.toJSONString(resultList));
        }
        return Result.genResult(ResultCodeEnum.SUCCESS.getCode(), "index2index success " + JSON.toJSONString(dataSyncConfigDto) + JSON.toJSONString(resultList), null);
    }

    public Result check(EsDataSyncConfigDto esDataSyncConfigDto) {
        //0，获取client
        String readIdx = esDataSyncConfigDto.getFrom().getIdx();
        String writeIdx = esDataSyncConfigDto.getTo().getIdx();
        String writeIdKey = esDataSyncConfigDto.getTo().getIdKey();
        //form
        if (StringUtils.isEmpty(esDataSyncConfigDto)) {
            log.error("index2index error readClient or readIdx is null:{}", JSON.toJSONString(esDataSyncConfigDto));
            return Result.genResult(ResultCodeEnum.PARAM_ERROR.getCode(), "serialDataSync error readClient or readIdx is null ", JSON.toJSONString(esDataSyncConfigDto));
        }

        //to
        if (StringUtils.isEmpty(writeIdx) || StringUtils.isEmpty(writeIdKey)) {
            log.error("index2index error writeClient or writeIdx or writeIdKey is null:{}", JSON.toJSONString(esDataSyncConfigDto));
            return Result.genResult(ResultCodeEnum.PARAM_ERROR.getCode(), "serialDataSync error writeClient or writeIdx or writeIdKey is null ", JSON.toJSONString(esDataSyncConfigDto));
        }

        if (!esOpsService.indexIsExist(readIdx)) {
            log.error("index2index error readClient readIdx is not exist:{}", JSON.toJSONString(esDataSyncConfigDto.getFrom()));
            return Result.genResult(ResultCodeEnum.PARAM_ERROR.getCode(), "serialDataSync error readClient readIdx is not exist ", JSON.toJSONString(esDataSyncConfigDto.getFrom()));
        }

        if (esOpsService.getIndexDocCount(readIdx) <= 0) {
            return Result.genResult(ResultCodeEnum.PARAM_ERROR.getCode(), "serialDataSync  readIdx doc count is 0 ", JSON.toJSONString(esDataSyncConfigDto.getFrom()));
        }

        if (!esOpsService.indexIsExist(writeIdx)) {
            log.error("index2index error writeClient writeIdx or writeIdKey is not exist:{}", JSON.toJSONString(esDataSyncConfigDto.getTo()));
            return Result.genResult(ResultCodeEnum.PARAM_ERROR.getCode(), "serialDataSync error writeClient writeIdx or writeIdKey is not exist ", JSON.toJSONString(esDataSyncConfigDto.getTo()));
        }

        return null;
    }
}
