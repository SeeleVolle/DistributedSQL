package com.minisql.master.api;

import lombok.Getter;

@Getter
public enum StatusCode {
    OK(200, "OK"),
    CREATED(201, "Created"),
    ACCEPTED(202, "Accepted"),
    NO_CONTENT(204, "No Content"),

    BAD_REQUEST(400, "Bad request"),
    UNAUTHORIZED(401, "Unauthorized"),
    FORBIDDEN(403, "Forbidden"),
    NOT_FOUND(404, "Not found"),

    INTERNAL_SERVER_ERROR(500, "Internal server error"),
    NOT_IMPLEMENTED(501, "Not implemented"),
    SERVICE_UNAVAILABLE(503, "Service unavailable"),

    EXCEPTION(600, "Exception"),
    TABLE_EXIST(601, "Table exist"),
    TABLE_NOT_EXIST(602, "Table not exist");

    private final int status;
    private final String message;

    StatusCode(int status, String message) {
        this.status = status;
        this.message = message;
    }

}
