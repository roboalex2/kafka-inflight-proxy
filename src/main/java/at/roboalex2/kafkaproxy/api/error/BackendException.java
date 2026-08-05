package at.roboalex2.kafkaproxy.api.error;

public abstract class BackendException extends RuntimeException {
    private final BackendErrorCode errorCode;

    protected BackendException(BackendErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    protected BackendException(BackendErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public BackendErrorCode getErrorCode() { return errorCode; }
}
