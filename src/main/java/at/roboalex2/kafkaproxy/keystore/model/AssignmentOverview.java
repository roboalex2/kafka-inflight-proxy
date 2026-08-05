package at.roboalex2.kafkaproxy.keystore.model;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class AssignmentOverview {
    private final Map<UUID, Map<String, UUID>> topics;
    private final List<UUID> unassignedKeyIds;

    public AssignmentOverview(Map<UUID, Map<String, UUID>> topics, List<UUID> unassignedKeyIds) {
        this.topics = topics;
        this.unassignedKeyIds = unassignedKeyIds;
    }
    public Map<UUID, Map<String, UUID>> getTopics() { return topics; }
    public List<UUID> getUnassignedKeyIds() { return unassignedKeyIds; }
}
