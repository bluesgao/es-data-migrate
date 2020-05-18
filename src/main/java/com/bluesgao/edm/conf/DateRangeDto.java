package com.bluesgao.edm.conf;

import lombok.Getter;
import lombok.Setter;

/*闭区间
 */
@Setter
@Getter
public class DateRangeDto {
    private String dateField;
    private String start;//YYYY-MM-DD HH:mm:ss
    private String end;//YYYY-MM-DD HH:mm:ss
    private SplitDateType splitType;//分割类型
}
