package com.bluesgao.edm.conf;

import lombok.Getter;
import lombok.Setter;


/*
{
  "esDataSyncConfig": {
    "from": {
      "cluster": "esm",
      "idx": "cf-content-5",
    },
    "to": {
      "cluster": "jes",
      "idx": "cf-content-5",
      "idKey": "id"
    },
    "dateRangeDto": {
      "dateField": "esCreateDate",
      "start": "2019-08-01 00:00:00",
      "end": "2019-09-01 00:00:00"
      "splitType":"BY_HOUR"
    }
  }
}
 */
@Getter
@Setter
public class EsDataSyncConfigDto {
    private IdxDto from;
    private IdxDto to;
    private DateRangeDto dateRangeDto;
}
