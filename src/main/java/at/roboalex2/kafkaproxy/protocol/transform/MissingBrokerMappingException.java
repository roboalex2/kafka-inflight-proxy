package at.roboalex2.kafkaproxy.protocol.transform;

import at.roboalex2.kafkaproxy.config.Endpoint;

public class MissingBrokerMappingException extends RuntimeException {
    private final Endpoint missingEndpoint;

    public MissingBrokerMappingException(Endpoint missingEndpoint) {
        super("No proxy endpoint is configured for advertised Kafka broker " + missingEndpoint);
        this.missingEndpoint = missingEndpoint;
    }

    public Endpoint getMissingEndpoint() { return missingEndpoint; }
}
