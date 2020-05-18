package com.bluesgao.edm.service;


import com.bluesgao.edm.conf.DateRangeDto;
import com.bluesgao.edm.conf.SplitDateType;
import com.bluesgao.edm.util.DateUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class SplitStrategy {
    public Map<String, DateRangeDto> splitByEveryDate(Date startDate, Date endDate, String dateField) {
        int between = 0;
        try {
            between = DateUtils.dateBetween(startDate, endDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        Map<String, DateRangeDto> resultMap = new HashMap<>();

        for (int i = 0; i <= between; i++) {
            Date tempDate = DateUtils.dateAddDays(startDate, i);
            String dateKey = null;
            try {
                dateKey = DateUtils.dateFormat(tempDate, DateUtils.DATE_PATTERN);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            if (dateKey != null) {
                DateRangeDto dateRangeDto = new DateRangeDto();
                dateRangeDto.setStart(dateKey + " 00:00:00");
                dateRangeDto.setEnd(dateKey + " 23:59:59");
                dateRangeDto.setDateField(dateField);
                resultMap.put(dateKey, dateRangeDto);
            }
        }
        return resultMap;
    }

    public Map<String, DateRangeDto> splitRangeDateByType(Date startDate, Date endDate, String dateField, SplitDateType type) {
        Map<String, DateRangeDto> resultMap = new HashMap<>();
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
                    DateRangeDto dateRangeDto = new DateRangeDto();
                    dateRangeDto.setStart(yyyyMMddStr + DateUtils.DAY_BEGIN_TIME);
                    dateRangeDto.setEnd(yyyyMMddStr + DateUtils.DAY_END_TIME);
                    dateRangeDto.setDateField(dateField);
                    dateRangeDto.setSplitType(SplitDateType.BY_DATE);
                    resultMap.put(yyyyMMddStr, dateRangeDto);
                } else if (type.getCode().equals(SplitDateType.BY_HOUR.getCode())) {
                    List<Map<Date, Date>> hoursMapList = DateUtils.splitDateByHours(tempDate);
                    for (Map<Date, Date> hourMap : hoursMapList) {
                        for (Date key : hourMap.keySet()) {
                            try {
                                String yyyyMMddHHStr = DateUtils.dateFormat(key, DateUtils.DATE_HOUR_PATTERN);
                                DateRangeDto dateRangeDto = new DateRangeDto();
                                dateRangeDto.setStart(DateUtils.dateFormat(key, DateUtils.DATE_TIME_PATTERN));
                                dateRangeDto.setEnd(DateUtils.dateFormat(hourMap.get(key), DateUtils.DATE_TIME_PATTERN));
                                dateRangeDto.setDateField(dateField);
                                dateRangeDto.setSplitType(SplitDateType.BY_HOUR);
                                if (dateRangeDto != null && yyyyMMddHHStr != null && dateRangeDto.getStart() != null && dateRangeDto.getEnd() != null) {
                                    resultMap.put(yyyyMMddHHStr, dateRangeDto);
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
