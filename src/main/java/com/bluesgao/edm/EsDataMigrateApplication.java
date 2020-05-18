package com.bluesgao.edm;

import com.bluesgao.edm.condition.DataMigrateCondition;
import com.bluesgao.edm.condition.SplitCondition;
import com.bluesgao.edm.condition.SplitDateType;
import com.bluesgao.edm.service.impl.DataMigrateServiceImpl;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class EsDataMigrateApplication {

    public static void main(String[] args) {
        SpringApplication.run(EsDataMigrateApplication.class, args);

        DataMigrateCondition condition = new DataMigrateCondition();
        condition.setSourceIndex("");
        SplitCondition splitCondition = new SplitCondition();
        splitCondition.setDateField("");
        splitCondition.setDateField("create_at");
        splitCondition.setStart("2020-05-19 00:00:00");
        splitCondition.setEnd("2020-05-20 23:59:59");
        splitCondition.setSplitType(SplitDateType.BY_DATE);

        condition.setSplitCondition(splitCondition);
        new DataMigrateServiceImpl().indexMigrate(condition);
    }

}
