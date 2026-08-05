package at.roboalex2.kafkaproxy.protocol.inspect;

public enum TrafficDirection {
    CLIENT_TO_BROKER("C -> B", "Request"),
    BROKER_TO_CLIENT("B -> C", "Response");

    private final String label;
    private final String messageType;

    TrafficDirection(String label, String messageType) {
        this.label = label;
        this.messageType = messageType;
    }

    public String getLabel() { return label; }
    public String getMessageType() { return messageType; }
}
