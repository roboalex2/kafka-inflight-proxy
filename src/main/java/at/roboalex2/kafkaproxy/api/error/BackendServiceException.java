package at.roboalex2.kafkaproxy.api.error;

public class BackendServiceException extends BackendException {
    public BackendServiceException(BackendErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public BackendServiceException(BackendErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
