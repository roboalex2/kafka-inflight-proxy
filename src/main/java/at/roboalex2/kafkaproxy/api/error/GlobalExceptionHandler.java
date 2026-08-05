package at.roboalex2.kafkaproxy.api.error;

import at.roboalex2.kafkaproxy.api.generated.model.BackendErrorResponse;
import at.roboalex2.kafkaproxy.config.KafkaProxyProperties;
import jakarta.validation.ConstraintViolationException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.method.MethodValidationException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

@RestControllerAdvice
public class GlobalExceptionHandler {
    private final List<String> secrets;

    public GlobalExceptionHandler(KafkaProxyProperties properties) {
        this.secrets = new ArrayList<>();
        addSecret(properties.getCrypto().getKeyEncryptionKey());
        addSecret(properties.getRedis().getPassword());
    }

    @ExceptionHandler(BackendException.class)
    public ResponseEntity<BackendErrorResponse> backendException(BackendException exception) {
        return response(exception.getErrorCode(), exception);
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<BackendErrorResponse> typeMismatch(MethodArgumentTypeMismatchException exception) {
        BackendErrorCode code = "topicId".equals(exception.getName())
                ? BackendErrorCode.INVALID_TOPIC_ID : BackendErrorCode.INVALID_ASSIGNMENT_REQUEST;
        return response(code, exception);
    }

    @ExceptionHandler({MethodArgumentNotValidException.class, HandlerMethodValidationException.class,
            MethodValidationException.class, ConstraintViolationException.class,
            HttpMessageNotReadableException.class})
    public ResponseEntity<BackendErrorResponse> invalidRequest(Exception exception) {
        BackendErrorCode code = exception.getMessage() != null && exception.getMessage().contains("recordKeyHash")
                ? BackendErrorCode.INVALID_RECORD_KEY_HASH : BackendErrorCode.INVALID_ASSIGNMENT_REQUEST;
        return response(code, exception);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<BackendErrorResponse> unknownException(Exception exception) {
        return response(BackendErrorCode.INTERNAL_SERVER_ERROR, exception);
    }

    private ResponseEntity<BackendErrorResponse> response(BackendErrorCode code, Exception exception) {
        String message = exception.getMessage() == null ? code.name() : exception.getMessage();
        BackendErrorResponse body = new BackendErrorResponse(code.name(), redact(message), redact(stackTrace(exception)));
        return ResponseEntity.status(code.getHttpStatus()).body(body);
    }

    private String stackTrace(Exception exception) {
        StringWriter output = new StringWriter();
        exception.printStackTrace(new PrintWriter(output));
        return output.toString();
    }

    private String redact(String value) {
        String redacted = value;
        for (String secret : secrets) redacted = redacted.replace(secret, "[REDACTED]");
        return redacted;
    }

    private void addSecret(String secret) {
        if (secret != null && !secret.isBlank()) secrets.add(secret);
    }
}
