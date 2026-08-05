package at.roboalex2.kafkaproxy.keystore.service;

import at.roboalex2.kafkaproxy.api.error.BackendErrorCode;
import at.roboalex2.kafkaproxy.api.error.BackendServiceException;
import at.roboalex2.kafkaproxy.keystore.model.AssignmentChange;
import at.roboalex2.kafkaproxy.keystore.model.AssignmentId;
import at.roboalex2.kafkaproxy.keystore.model.AssignmentOverview;
import at.roboalex2.kafkaproxy.keystore.model.DeletedKey;
import at.roboalex2.kafkaproxy.keystore.model.KeyAssignment;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class CryptoKeyManagementService {
    private final DataEncryptionKeyService keys;
    private final KeyAssignmentService assignments;

    public CryptoKeyManagementService(DataEncryptionKeyService keys, KeyAssignmentService assignments) {
        this.keys = keys;
        this.assignments = assignments;
    }

    public AssignmentOverview list() {
        List<KeyAssignment> storedAssignments = assignments.findAll();
        Set<UUID> allKeyIds = new LinkedHashSet<>(keys.findAllKeyIds());
        Map<UUID, Map<String, UUID>> topics = new TreeMap<>((left, right) ->
                left.toString().compareTo(right.toString()));
        Set<UUID> assignedKeyIds = new LinkedHashSet<>();
        for (KeyAssignment assignment : storedAssignments) {
            if (!allKeyIds.contains(assignment.getKeyId())) {
                throw new BackendServiceException(BackendErrorCode.KEY_MATERIAL_NOT_FOUND,
                        "Assignment references missing cryptographic key " + assignment.getKeyId());
            }
            topics.computeIfAbsent(assignment.getAssignmentId().getTopicId(), ignored -> new TreeMap<>())
                    .put(assignment.getAssignmentId().getRecordKeyHash(), assignment.getKeyId());
            assignedKeyIds.add(assignment.getKeyId());
        }
        List<UUID> unassigned = allKeyIds.stream().filter(keyId -> !assignedKeyIds.contains(keyId))
                .sorted((left, right) -> left.toString().compareTo(right.toString())).toList();
        Map<UUID, Map<String, UUID>> deterministic = new LinkedHashMap<>();
        topics.forEach((topic, hashes) -> deterministic.put(topic, new LinkedHashMap<>(hashes)));
        return new AssignmentOverview(deterministic, unassigned);
    }

    public AssignmentChange assignExisting(AssignmentId id, UUID keyId) {
        if (!keys.exists(keyId)) {
            throw new BackendServiceException(BackendErrorCode.KEY_NOT_FOUND,
                    "Cryptographic key " + keyId + " was not found");
        }
        return change(id, keyId, false);
    }

    public AssignmentChange generateAndAssign(AssignmentId id) {
        return change(id, keys.generateAndStore(), true);
    }

    public DeletedKey delete(UUID keyId) {
        if (!keys.exists(keyId)) {
            throw new BackendServiceException(BackendErrorCode.KEY_NOT_FOUND,
                    "Cryptographic key " + keyId + " was not found");
        }
        List<AssignmentId> indexed = assignments.findIndexedAssignments(keyId);
        List<AssignmentId> removed = new ArrayList<>();
        for (AssignmentId id : indexed) {
            if (assignments.find(id).map(KeyAssignment::getKeyId).filter(keyId::equals).isPresent()) {
                assignments.deleteIfAssignedTo(id, keyId);
                removed.add(id);
            } else {
                assignments.deleteIfAssignedTo(id, keyId);
            }
        }
        keys.deleteRequired(keyId);
        assignments.deleteIndex(keyId);
        return new DeletedKey(keyId, removed);
    }

    private AssignmentChange change(AssignmentId id, UUID keyId, boolean generated) {
        UUID previous = assignments.assign(id, keyId).orElse(null);
        UUID replaced = previous != null && !previous.equals(keyId) ? previous : null;
        return new AssignmentChange(id, keyId, generated, replaced);
    }
}
