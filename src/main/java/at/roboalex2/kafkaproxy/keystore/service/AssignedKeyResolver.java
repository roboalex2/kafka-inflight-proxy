package at.roboalex2.kafkaproxy.keystore.service;

import at.roboalex2.kafkaproxy.api.error.BackendErrorCode;
import at.roboalex2.kafkaproxy.api.error.BackendServiceException;
import at.roboalex2.kafkaproxy.keystore.model.AssignedKey;
import at.roboalex2.kafkaproxy.keystore.model.AssignmentId;
import at.roboalex2.kafkaproxy.keystore.model.KeyAssignment;
import org.springframework.stereotype.Service;

/** Performs fresh repository reads on every call; deliberately contains no assignment or DEK cache. */
@Service
public class AssignedKeyResolver {
    private final KeyAssignmentService assignments;
    private final DataEncryptionKeyService keys;

    public AssignedKeyResolver(KeyAssignmentService assignments, DataEncryptionKeyService keys) {
        this.assignments = assignments;
        this.keys = keys;
    }

    public AssignedKey resolve(AssignmentId id) {
        KeyAssignment assignment = assignments.find(id).orElseThrow(() ->
                new BackendServiceException(BackendErrorCode.ASSIGNMENT_NOT_FOUND, "Key assignment was not found"));
        return keys.resolve(assignment.getKeyId()).orElseThrow(() ->
                new BackendServiceException(BackendErrorCode.KEY_MATERIAL_NOT_FOUND,
                        "Assigned cryptographic key material was not found"));
    }

    public AssignedKey getOrCreate(AssignmentId id) {
        KeyAssignment assignment = assignments.find(id).orElse(null);
        if (assignment != null) {
            AssignedKey existing = keys.resolve(assignment.getKeyId()).orElse(null);
            if (existing != null) return existing;
        }
        AssignedKey generated = keys.generateAndStoreForImmediateUse();
        assignments.assign(id, generated.getKeyId());
        return generated;
    }
}
