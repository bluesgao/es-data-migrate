package com.bluesgao.edm.util;

public class OtherUtils {
    public static String genRedisKey(String... strings) {
        if (strings.length > 0) {
            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer.append(Constants.APP_NAME);
            for (String str : strings) {
                stringBuffer.append(Constants.SPLIT_TOKEN).append(str);
            }
            return stringBuffer.toString();
        }
        return null;
    }

}
