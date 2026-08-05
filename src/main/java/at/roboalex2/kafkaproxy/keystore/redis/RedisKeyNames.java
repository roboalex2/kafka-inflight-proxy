package at.roboalex2.kafkaproxy.keystore.redis;

import at.roboalex2.kafkaproxy.keystore.model.AssignmentId;
import java.util.UUID;

public final class RedisKeyNames {
    public static final String KEYS_HASH = "kafka-proxy:crypto:keys";
    public static final String ASSIGNMENTS_PREFIX = "kafka-proxy:crypto:assignments:";
    public static final String KEY_ASSIGNMENT_INDEX_PREFIX = "kafka-proxy:crypto:key-assignment-index:";

    private RedisKeyNames() { }

    public static String assignments(UUID topicId) { return ASSIGNMENTS_PREFIX + topicId; }
    public static String assignmentIndex(UUID keyId) { return KEY_ASSIGNMENT_INDEX_PREFIX + keyId; }
    public static String indexMember(AssignmentId id) {
        return id.getTopicId() + "|" + id.getRecordKeyHash();
    }
    public static AssignmentId parseIndexMember(String member) {
        int separator = member.indexOf('|');
        if (separator <= 0 || separator == member.length() - 1) {
            throw new IllegalArgumentException("Malformed Redis assignment index member");
        }
        return new AssignmentId(UUID.fromString(member.substring(0, separator)), member.substring(separator + 1));
    }
}
