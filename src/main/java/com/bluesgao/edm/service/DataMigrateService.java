package com.bluesgao.edm.service;

import com.bluesgao.edm.common.Result;
import com.bluesgao.edm.condition.DataMigrateCondition;

public interface DataMigrateService {
    Result indexMigrate(DataMigrateCondition conditon);
}
