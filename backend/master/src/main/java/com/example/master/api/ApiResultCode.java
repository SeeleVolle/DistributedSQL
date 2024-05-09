package com.example.master.api;

public enum ApiResultCode {
    SUCCESS(200, "success"),
    BAD_REQUEST(400, "bad request"),
    NOT_FOUND(404, "not found"),
    INTERNAL_SERVER_ERROR(500, "internal server error"),
    TABLE_EXIST(801, "Table exist"),
    TABLE_NOT_EXIST(802, "Table not exist"),
    FAILED(803, "Failed");

    private final int code;
    private final String message;

    ApiResultCode(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }
}
