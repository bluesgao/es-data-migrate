package com.bluesgao.edm.conf;

public enum SplitDateType {
    BY_DATE("BY_DATE", "按日"),
    BY_HOUR("BY_HOUR", "按小时");
    String code;
    String msg;

    SplitDateType(String code, String msg) {
        this.code = code;
        this.msg = msg;
    }


    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public static SplitDateType getByCode(String code) {
        for (SplitDateType type : values()) {
            if (type.getCode().equals(code)) {
                return type;
            }
        }
        return null;
    }
}
