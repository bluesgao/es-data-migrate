package com.bluesgao.edm.service;

import java.util.Map;

public interface CacheOpsService {

    // 加入元素
    void setValue(String key, Map<String, Object> value);

    // 加入元素
    void setValue(String key, String value);

    // 加入元素
    void setValue(String key, Object value);

    // 获取元素
    Object getMapValue(String key);

    // 获取元素
    Object getValue(String key);

    boolean hsetValue(String key, String field, Object value );

    Object hgetValue(String key, String field);
}
