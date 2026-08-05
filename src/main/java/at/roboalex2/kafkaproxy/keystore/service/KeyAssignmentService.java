package at.roboalex2.kafkaproxy.keystore.service;

import at.roboalex2.kafkaproxy.keystore.model.AssignmentId;
import at.roboalex2.kafkaproxy.keystore.model.KeyAssignment;
import at.roboalex2.kafkaproxy.keystore.repository.KeyAssignmentRepository;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class KeyAssignmentService {
    private final KeyAssignmentRepository repository;
    public KeyAssignmentService(KeyAssignmentRepository repository) { this.repository = repository; }

    public Optional<KeyAssignment> find(AssignmentId id) { return repository.find(id); }
    public Optional<UUID> assign(AssignmentId id, UUID keyId) { return repository.assign(id, keyId); }
    public List<KeyAssignment> findAll() { return repository.findAll(); }
    public List<AssignmentId> findIndexedAssignments(UUID keyId) {
        return repository.findIndexedAssignments(keyId);
    }
    public void deleteIfAssignedTo(AssignmentId id, UUID keyId) {
        repository.deleteIfAssignedTo(id, keyId);
    }
    public void deleteIndex(UUID keyId) { repository.deleteIndex(keyId); }
}
