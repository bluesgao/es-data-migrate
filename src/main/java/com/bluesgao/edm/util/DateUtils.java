package com.bluesgao.edm.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * @desc 日期工具类
 **/
public class DateUtils {

    public static final String DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
    public static final String DATE_PATTERN = "yyyy-MM-dd";
    public static final String DATE_PATTERN_1 = "yyyyMMdd";
    public static final String MONTH_PATTERN = "yyyy-MM";
    public static final String DATE_HOUR_PATTERN = "yyyyMMddHH";
    public static final String DAY_BEGIN_TIME = " 00:00:00";
    public static final String DAY_END_TIME = " 23:59:59";

    /**
     * 时间格式化成字符串
     *
     * @param date    Date
     * @param pattern StrUtils.DATE_TIME_PATTERN || StrUtils.DATE_PATTERN， 如果为空，则为yyyy-MM-dd
     * @return
     * @throws ParseException
     */
    public static String dateFormat(Date date, String pattern) throws ParseException {
        if (isBlank(pattern)) {
            pattern = DateUtils.DATE_PATTERN;
        }
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        return sdf.format(date);
    }

    public static boolean isBlank(String str) {
        if (str == null || str.length() == 0 || "".equals(str)) {
            return true;
        }
        return false;
    }

    /**
     * 分割日期为24小时
     */
    public static List<Map<Date, Date>> splitDateByHours(Date date) {
        if (date == null) {
            return null;
        }

        Date yyymmddDate = null;
        try {
            yyymmddDate = DateUtils.dateParse(DateUtils.dateFormat(date, DATE_PATTERN), DATE_PATTERN);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        if (yyymmddDate != null) {
            List<Map<Date, Date>> resultList = new ArrayList<>();
            for (int i = 0; i < 24; i++) {
                Map<Date, Date> hourRangeMap = new HashMap<>();
                Date start = DateUtils.dateAddHours(yyymmddDate, i);
                Date end = DateUtils.dateAddMinutes(start, 59);
                end = DateUtils.dateAddSeconds(end, 59);
                hourRangeMap.put(start, end);
                resultList.add(hourRangeMap);
            }
            return resultList;
        }
        return null;
    }

    /**
     * 时间加减天数
     *
     * @param startDate 要处理的时间，Null则为当前时间
     * @param days      加减的天数
     * @return Date
     */
    public static Date dateAddDays(Date startDate, int days) {
        if (startDate == null) {
            startDate = new Date();
        }
        Calendar c = Calendar.getInstance();
        c.setTime(startDate);
        c.set(Calendar.DATE, c.get(Calendar.DATE) + days);
        return c.getTime();
    }

    /**
     * 时间加减小时
     *
     * @param startDate 要处理的时间，Null则为当前时间
     * @param hours     加减的小时
     * @return Date
     */
    public static Date dateAddHours(Date startDate, int hours) {
        if (startDate == null) {
            startDate = new Date();
        }
        Calendar c = Calendar.getInstance();
        c.setTime(startDate);
        c.set(Calendar.HOUR, c.get(Calendar.HOUR) + hours);
        return c.getTime();
    }

    /**
     * 时间加减分钟
     *
     * @param startDate 要处理的时间，Null则为当前时间
     * @param minutes   加减的分钟
     * @return
     */
    public static Date dateAddMinutes(Date startDate, int minutes) {
        if (startDate == null) {
            startDate = new Date();
        }
        Calendar c = Calendar.getInstance();
        c.setTime(startDate);
        c.set(Calendar.MINUTE, c.get(Calendar.MINUTE) + minutes);
        return c.getTime();
    }

    /**
     * 时间加减秒数
     *
     * @param startDate 要处理的时间，Null则为当前时间
     * @param seconds   加减的秒数
     * @return
     */
    public static Date dateAddSeconds(Date startDate, int seconds) {
        if (startDate == null) {
            startDate = new Date();
        }
        Calendar c = Calendar.getInstance();
        c.setTime(startDate);
        c.set(Calendar.SECOND, c.get(Calendar.SECOND) + seconds);
        return c.getTime();
    }


    /**
     * 时间比较（如果myDate>compareDate返回1，<返回-1，相等返回0）
     *
     * @param myDate      时间
     * @param compareDate 要比较的时间
     * @return int
     */
    public static int dateCompare(Date myDate, Date compareDate) {
        Calendar myCal = Calendar.getInstance();
        Calendar compareCal = Calendar.getInstance();
        myCal.setTime(myDate);
        compareCal.setTime(compareDate);
        return myCal.compareTo(compareCal);
    }

    /**
     * 获取两个日期（不含时分秒）相差的天数，不包含今天
     *
     * @param startDate
     * @param endDate
     * @return
     * @throws ParseException
     */
    public static int dateBetween(Date startDate, Date endDate) throws ParseException {
        Date dateStart = dateParse(dateFormat(startDate, DATE_PATTERN), DATE_PATTERN);
        Date dateEnd = dateParse(dateFormat(endDate, DATE_PATTERN), DATE_PATTERN);
        return (int) ((dateEnd.getTime() - dateStart.getTime()) / 1000 / 60 / 60 / 24);
    }

    /**
     * 字符串解析成时间对象
     *
     * @param dateTimeString String
     * @param pattern        StrUtils.DATE_TIME_PATTERN || StrUtils.DATE_PATTERN，如果为空，则为yyyy-MM-dd
     * @return
     * @throws ParseException
     */
    public static Date dateParse(String dateTimeString, String pattern) throws ParseException {
        if (isBlank(pattern)) {
            pattern = DateUtils.DATE_PATTERN;
        }
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        return sdf.parse(dateTimeString);
    }

    /**
     * 获取两个日期（不含时分秒）相差的天数，包含今天
     *
     * @param startDate
     * @param endDate
     * @return
     * @throws ParseException
     */
    public static int dateBetweenIncludeToday(Date startDate, Date endDate) throws ParseException {
        return dateBetween(startDate, endDate) + 1;
    }

    /**
     * 日期相加减天数
     *
     * @param date        如果为Null，则为当前时间
     * @param days        加减天数
     * @param includeTime 是否包括时分秒,true表示包含
     * @return
     * @throws ParseException
     */
    public static Date dateAdd(Date date, int days, boolean includeTime) throws ParseException {
        if (date == null) {
            date = new Date();
        }
        if (!includeTime) {
            SimpleDateFormat sdf = new SimpleDateFormat(DateUtils.DATE_PATTERN);
            date = sdf.parse(sdf.format(date));
        }
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.DATE, days);
        return cal.getTime();
    }

    /**
     * @return 返回字符串时间，格式：yyyy-MM-dd HH:mm:ss
     * @Desc 获取当前日期
     */
    public static String getNowDateTimeString() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern(DATE_TIME_PATTERN));
    }

    /**
     * 很关键 不要改动
     *
     * @return 返回字符串时间，格式：yyyyMMdd
     * @Desc 获取当前日期
     */
    public static String getNowDateString() {
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_PATTERN_1);
        return sdf.format(new Date());
    }
}
