package com.bluesgao.edm.service.impl;

import com.bluesgao.edm.service.CacheOpsService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class CacheOpsServiceImpl implements CacheOpsService {
    @Autowired
    private RedisTemplate redisTemplate;

    @Override
    public void setValue(String key, Map<String, Object> value) {
        ValueOperations<String, Object> vo = redisTemplate.opsForValue();
        vo.set(key, value);
        redisTemplate.expire(key, 1, TimeUnit.HOURS); // 这里指的是1小时后失效
    }

    @Override
    public Object getValue(String key) {
        ValueOperations<String, String> vo = redisTemplate.opsForValue();
        return vo.get(key);
    }

    @Override
    public Object hgetValue(String key, String field) {
         return redisTemplate.opsForHash().get(key, field);
    }

    @Override
    public boolean hsetValue(String key, String field, Object value) {
        try {
            redisTemplate.opsForHash().put(key,field,value);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }

    @Override
    public void setValue(String key, String value) {
        ValueOperations<String, Object> vo = redisTemplate.opsForValue();
        vo.set(key, value);
        redisTemplate.expire(key, 1, TimeUnit.HOURS); // 这里指的是1小时后失效
    }

    @Override
    public void setValue(String key, Object value) {
        ValueOperations<String, Object> vo = redisTemplate.opsForValue();
        vo.set(key, value);
        redisTemplate.expire(key, 1, TimeUnit.HOURS); // 这里指的是1小时后失效
    }

    @Override
    public Object getMapValue(String key) {
        ValueOperations<String, String> vo = redisTemplate.opsForValue();
        return vo.get(key);
    }
}
