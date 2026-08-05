package at.roboalex2.kafkaproxy.keystore.model;

import java.util.UUID;

public class KeyAssignment {
    private final AssignmentId assignmentId;
    private final UUID keyId;

    public KeyAssignment(AssignmentId assignmentId, UUID keyId) {
        this.assignmentId = assignmentId;
        this.keyId = keyId;
    }

    public AssignmentId getAssignmentId() { return assignmentId; }
    public UUID getKeyId() { return keyId; }
}
