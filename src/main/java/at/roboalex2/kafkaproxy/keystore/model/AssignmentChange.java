package at.roboalex2.kafkaproxy.keystore.model;

import java.util.UUID;

public class AssignmentChange {
    private final AssignmentId assignmentId;
    private final UUID keyId;
    private final boolean generated;
    private final UUID replacedKeyId;

    public AssignmentChange(AssignmentId assignmentId, UUID keyId, boolean generated, UUID replacedKeyId) {
        this.assignmentId = assignmentId;
        this.keyId = keyId;
        this.generated = generated;
        this.replacedKeyId = replacedKeyId;
    }
    public AssignmentId getAssignmentId() { return assignmentId; }
    public UUID getKeyId() { return keyId; }
    public boolean isGenerated() { return generated; }
    public UUID getReplacedKeyId() { return replacedKeyId; }
}
