package at.roboalex2.kafkaproxy.api.error;

import org.springframework.http.HttpStatus;

public enum BackendErrorCode {
    KEY_NOT_FOUND(HttpStatus.NOT_FOUND),
    ASSIGNMENT_NOT_FOUND(HttpStatus.NOT_FOUND),
    KEY_MATERIAL_NOT_FOUND(HttpStatus.NOT_FOUND),
    INVALID_TOPIC_ID(HttpStatus.BAD_REQUEST),
    INVALID_RECORD_KEY_HASH(HttpStatus.BAD_REQUEST),
    INVALID_ASSIGNMENT_REQUEST(HttpStatus.BAD_REQUEST),
    RESERVED_HEADER_CONFLICT(HttpStatus.BAD_REQUEST),
    CRYPTOGRAPHIC_OPERATION_FAILED(HttpStatus.INTERNAL_SERVER_ERROR),
    REDIS_OPERATION_FAILED(HttpStatus.INTERNAL_SERVER_ERROR),
    PROTOCOL_TRANSFORMATION_FAILED(HttpStatus.INTERNAL_SERVER_ERROR),
    INTERNAL_SERVER_ERROR(HttpStatus.INTERNAL_SERVER_ERROR);

    private final HttpStatus httpStatus;

    BackendErrorCode(HttpStatus httpStatus) { this.httpStatus = httpStatus; }
    public HttpStatus getHttpStatus() { return httpStatus; }
}
