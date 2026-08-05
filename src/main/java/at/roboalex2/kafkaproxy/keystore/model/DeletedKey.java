package at.roboalex2.kafkaproxy.keystore.model;

import java.util.List;
import java.util.UUID;

public class DeletedKey {
    private final UUID keyId;
    private final List<AssignmentId> removedAssignments;

    public DeletedKey(UUID keyId, List<AssignmentId> removedAssignments) {
        this.keyId = keyId;
        this.removedAssignments = List.copyOf(removedAssignments);
    }
    public UUID getKeyId() { return keyId; }
    public List<AssignmentId> getRemovedAssignments() { return removedAssignments; }
}
