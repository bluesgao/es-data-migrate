package com.bluesgao.edm.condition;

import lombok.Getter;
import lombok.Setter;


/*
 {
      "dateField": "esCreateDate",
      "start": "2019-08-01 00:00:00",
      "end": "2019-09-01 00:00:00"
      "splitType":"BY_HOUR"
    }
 */
@Getter
@Setter
public class SplitCondition {
    private String dateField;
    private String start;//YYYY-MM-DD HH:mm:ss
    private String end;//YYYY-MM-DD HH:mm:ss
    private SplitDateType splitType;//分割类型
}
