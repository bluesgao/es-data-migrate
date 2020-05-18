package com.bluesgao.edm.common;

import lombok.Data;

import java.io.Serializable;

@Data
public class Result<T> implements Serializable {
    private static final long serialVersionUID = 4601299480738318008L;
    private String resultCode;
    private String resultMessage;
    private T data;

    public static <T> Result<T> genResult(String code, String message, T data) {
        Result<T> result = new Result();
        result.setResultCode(code);
        result.setData(data);
        result.setResultMessage(message);
        return result;
    }
}

