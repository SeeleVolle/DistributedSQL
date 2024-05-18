package com.example.master.api;

import lombok.Data;

@Data
public class ApiResult {
    private int status;
    private String message;
    private Object data;

    public ApiResult() {
        this.status = StatusCode.OK.getStatus();
        this.message = StatusCode.OK.getMessage();
    }

    public ApiResult(Object data) {
        this();
        this.data = data;
    }

    public ApiResult(int status, String message) {
        this.status = status;
        this.message = message;
    }

    public ApiResult(int status, String message, Object data) {
        this.status = status;
        this.message = message;
        this.data = data;
    }

    public ApiResult ok() {
        this.status = StatusCode.OK.getStatus();
        this.message = StatusCode.OK.getMessage();
        return this;
    }

    public ApiResult failed() {
        this.status = StatusCode.EXCEPTION.getStatus();
        this.message = StatusCode.EXCEPTION.getMessage();
        return this;
    }

    public ApiResult message(String message) {
        this.message = message;
        return this;
    }

    public ApiResult data(Object data) {
        this.data = data;
        return this;
    }
}
