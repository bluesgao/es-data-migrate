package com.bluesgao.edm.service;

import com.bluesgao.edm.EsDataMigrateApplication;
import com.bluesgao.edm.condition.DataMigrateCondition;
import com.bluesgao.edm.condition.SplitCondition;
import com.bluesgao.edm.condition.SplitDateType;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {EsDataMigrateApplication.class})// 指定启动类
@Slf4j
class DataMigrateServiceTest {

    @Resource
    DataMigrateService dataMigrateService;

    @Test
    void indexMigrate() {
        DataMigrateCondition condition = new DataMigrateCondition();
        condition.setSourceIndex("order");
        condition.setDestinationIndex("order_v2");
        condition.setIdKey("id");
        SplitCondition splitCondition = new SplitCondition();
        splitCondition.setDateField("orderTime");
        splitCondition.setStart("2020-01-01 00:00:00");
        splitCondition.setEnd("2020-01-01 23:59:59");
        splitCondition.setSplitType(SplitDateType.BY_DATE);

        condition.setSplitCondition(splitCondition);
        dataMigrateService.indexMigrate(condition);
    }
}