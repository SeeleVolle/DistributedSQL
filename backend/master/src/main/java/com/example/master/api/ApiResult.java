package com.example.master.api;

import lombok.Data;

@Data
public class ApiResult {
    private int code;
    private String message;
    private Object data;

    public ApiResult() {
        this.code = ApiResultCode.SUCCESS.getCode();
        this.message = ApiResultCode.SUCCESS.getMessage();
    }

    public ApiResult(Object data) {
        this();
        this.data = data;
    }

    public ApiResult(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public ApiResult(int code, String message, Object data) {
        this.code = code;
        this.message = message;
        this.data = data;
    }
}
