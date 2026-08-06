package at.roboalex2.kafkaproxy.protocol.topic;

import at.roboalex2.kafkaproxy.api.error.BackendErrorCode;
import at.roboalex2.kafkaproxy.api.error.BackendServiceException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.MetadataResponseData;
import org.springframework.stereotype.Component;

/** Keeps the non-secret topic name/UUID mapping learned from Kafka Metadata. */
@Component
public class TopicIdentityResolver {
    private final ConcurrentHashMap<String, UUID> topicIdsByName = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, String> topicNamesById = new ConcurrentHashMap<>();

    public void observe(MetadataResponseData metadata) {
        for (MetadataResponseData.MetadataResponseTopic topic : metadata.topics()) {
            observe(topic.name(), topic.topicId());
        }
    }

    public void observe(String topicName, Uuid protocolTopicId) {
        UUID topicId = toJavaUuid(protocolTopicId);
        if (topicId != null && topicName != null && !topicName.isBlank()) {
            topicIdsByName.put(topicName, topicId);
            topicNamesById.put(topicId, topicName);
        }
    }

    public UUID resolveRequired(String topicName, Uuid protocolTopicId) {
        UUID topicId = toJavaUuid(protocolTopicId);
        if (topicId != null) {
            if (topicName != null && !topicName.isBlank()) observe(topicName, protocolTopicId);
            return topicId;
        }
        UUID resolved = topicName == null ? null : topicIdsByName.get(topicName);
        if (resolved == null) {
            throw new BackendServiceException(BackendErrorCode.PROTOCOL_TRANSFORMATION_FAILED,
                    "Kafka topic UUID is unavailable for topic " + safeTopicName(topicName));
        }
        return resolved;
    }

    public Optional<String> resolveName(UUID topicId) {
        return Optional.ofNullable(topicNamesById.get(topicId));
    }

    private UUID toJavaUuid(Uuid topicId) {
        if (topicId == null || Uuid.ZERO_UUID.equals(topicId)) return null;
        return new UUID(topicId.getMostSignificantBits(), topicId.getLeastSignificantBits());
    }

    private String safeTopicName(String topicName) {
        return topicName == null || topicName.isBlank() ? "<unnamed>" : topicName;
    }
}
