package at.roboalex2.kafkaproxy.keystore.model;

import java.util.Objects;
import java.util.UUID;

public class AssignmentId implements Comparable<AssignmentId> {
    private final UUID topicId;
    private final String recordKeyHash;

    public AssignmentId(UUID topicId, String recordKeyHash) {
        this.topicId = Objects.requireNonNull(topicId);
        this.recordKeyHash = Objects.requireNonNull(recordKeyHash);
    }

    public UUID getTopicId() { return topicId; }
    public String getRecordKeyHash() { return recordKeyHash; }

    @Override public int compareTo(AssignmentId other) {
        int topicComparison = topicId.toString().compareTo(other.topicId.toString());
        return topicComparison != 0 ? topicComparison : recordKeyHash.compareTo(other.recordKeyHash);
    }
    @Override public boolean equals(Object other) {
        return other instanceof AssignmentId id && topicId.equals(id.topicId)
                && recordKeyHash.equals(id.recordKeyHash);
    }
    @Override public int hashCode() { return Objects.hash(topicId, recordKeyHash); }
}
