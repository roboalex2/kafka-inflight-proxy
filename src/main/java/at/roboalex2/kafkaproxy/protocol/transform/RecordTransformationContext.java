package at.roboalex2.kafkaproxy.protocol.transform;

import java.util.UUID;

public record RecordTransformationContext(UUID topicId, String topicName, int partition) { }
