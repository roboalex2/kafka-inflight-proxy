package at.roboalex2.kafkaproxy.keystore.repository;

import at.roboalex2.kafkaproxy.keystore.model.AssignmentId;
import at.roboalex2.kafkaproxy.keystore.model.KeyAssignment;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface KeyAssignmentRepository {
    Optional<KeyAssignment> find(AssignmentId assignmentId);
    Optional<UUID> assign(AssignmentId assignmentId, UUID keyId);
    void deleteIfAssignedTo(AssignmentId assignmentId, UUID keyId);
    List<KeyAssignment> findAll();
    List<AssignmentId> findIndexedAssignments(UUID keyId);
    void deleteIndex(UUID keyId);
}
