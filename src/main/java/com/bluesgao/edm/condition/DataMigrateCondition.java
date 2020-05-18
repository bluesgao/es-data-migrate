package com.bluesgao.edm.condition;

import lombok.Getter;
import lombok.Setter;


/*
 {
      "sourceIndex": "cf-content-5",
      "destinationIndex": "cf-content-6",
      "idKey":"id",
      "splitCondition":{
      "dateField": "esCreateDate",
      "start": "2019-08-01 00:00:00",
      "end": "2019-09-01 00:00:00"
      "splitType":"BY_HOUR"
      }
    }
 */
@Getter
@Setter
public class DataMigrateCondition {
    String sourceIndex;
    String destinationIndex;
    String idKey;
    SplitCondition splitCondition;
}
