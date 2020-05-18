package com.bluesgao.edm.common;

public enum ResultCodeEnum {
    SUCCESS("200", "成功"),
    SYSTEM_ERR("10000", "系统错误"),
    UNAUTHORIZED("10100", "permission denied"),
    REJECT_TOKEN("10101", "token校验不通过"),
    PARAM_ERROR("10200", "请求参数错误"),
    PARAM_NOT_NULL("10201", "参数必填"),
    PARAM_DATE_ERROR("10202", "参数错误，请使用日期格式:yyyy-MM-dd"),
    PARAM_DATE_DD1("10203", "参数格式错误，日期格式应该是yyyy-MM-dd"),
    PARAM_DATE_DD2("10204", "参数格式错误，日期格式应该是yyyy-MM-dd"),
    PARAM_DATE_DD3("10205", "参数格式错误，日期格式应该是yyyy-MM-dd HH:mm:ss"),
    PARAM_DATE_GT_RETURN("10205", "参数格式错误，起飞日期大于返回日期"),
    PARAM_NOT_SUPPORT("10502", "不支持的参数"),
    THRIFT_ERROR("50100", "RPC服务调用异常"),
    THRIFT_RET_NULL("50101", "RPC服务返回空"),
    THRIFT_RET_ERROR("50102", "RPC服务调用错误"),
    THRIFT_TIMEOUT("50103", "RPC服务调用超时"),
    ORDER_JSF_EXCEPTION("21001", "订单JSF异常"),
    ORDER_DAO_EXCEPTION("22001", "订单DAO查询异常"),
    ORDER_WEB_JSF_ERROR("23001", "订单前台系统服务异常"),
    DAO_EXCEPTION("21000", "DAO层异常"),
    JSF_EXCEPTION("22000", "JSF层逻辑异常"),
    WEB_EXCEPTION("23000", "WEB系统业务异常");

    private String code;
    private String message;

    private ResultCodeEnum(String code, String message) {
        this.code = code;
        this.message = message;
    }

    public String getCode() {
        return this.code;
    }

    public String getMessage() {
        return this.message;
    }
}