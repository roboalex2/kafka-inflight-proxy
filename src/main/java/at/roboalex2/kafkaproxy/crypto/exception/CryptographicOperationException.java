package at.roboalex2.kafkaproxy.crypto.exception;

import at.roboalex2.kafkaproxy.api.error.BackendErrorCode;
import at.roboalex2.kafkaproxy.api.error.BackendException;

public class CryptographicOperationException extends BackendException {
    public CryptographicOperationException(String message, Throwable cause) {
        super(BackendErrorCode.CRYPTOGRAPHIC_OPERATION_FAILED, message, cause);
    }

    public CryptographicOperationException(String message) {
        super(BackendErrorCode.CRYPTOGRAPHIC_OPERATION_FAILED, message);
    }
}
