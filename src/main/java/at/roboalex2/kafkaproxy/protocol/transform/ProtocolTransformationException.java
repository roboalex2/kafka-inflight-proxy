package at.roboalex2.kafkaproxy.protocol.transform;

public class ProtocolTransformationException extends RuntimeException {
    public ProtocolTransformationException(String message) {
        super(message);
    }

    public ProtocolTransformationException(String message, Throwable cause) {
        super(message, cause);
    }
}
