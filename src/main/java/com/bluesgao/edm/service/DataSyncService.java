package com.bluesgao.edm.service;

import com.bluesgao.edm.common.Result;
import com.bluesgao.edm.conf.EsDataSyncConfigDto;

public interface DataSyncService {
    Result index2index(EsDataSyncConfigDto dataSyncConfigDto);
}
