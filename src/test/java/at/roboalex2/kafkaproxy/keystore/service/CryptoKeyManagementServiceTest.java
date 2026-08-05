package at.roboalex2.kafkaproxy.keystore.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import at.roboalex2.kafkaproxy.api.error.BackendErrorCode;
import at.roboalex2.kafkaproxy.api.error.BackendException;
import at.roboalex2.kafkaproxy.config.KafkaProxyProperties;
import at.roboalex2.kafkaproxy.crypto.envelope.AesGcmEnvelopeCodec;
import at.roboalex2.kafkaproxy.crypto.key.DataEncryptionKeyGenerator;
import at.roboalex2.kafkaproxy.crypto.key.KeyEncryptionService;
import at.roboalex2.kafkaproxy.keystore.model.AssignedKey;
import at.roboalex2.kafkaproxy.keystore.model.AssignmentChange;
import at.roboalex2.kafkaproxy.keystore.model.AssignmentId;
import at.roboalex2.kafkaproxy.keystore.model.CryptoKeyEntity;
import at.roboalex2.kafkaproxy.keystore.model.DeletedKey;
import at.roboalex2.kafkaproxy.keystore.model.KeyAssignment;
import at.roboalex2.kafkaproxy.keystore.repository.CryptoKeyRepository;
import at.roboalex2.kafkaproxy.keystore.repository.KeyAssignmentRepository;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CryptoKeyManagementServiceTest {
    private final MemoryKeyRepository keyRepository = new MemoryKeyRepository();
    private final MemoryAssignmentRepository assignmentRepository = new MemoryAssignmentRepository();
    private DataEncryptionKeyService keys;
    private KeyAssignmentService assignments;
    private CryptoKeyManagementService management;

    @BeforeEach
    void setUp() {
        KafkaProxyProperties properties = new KafkaProxyProperties();
        properties.getCrypto().setKeyEncryptionKey("test-kek-never-persisted");
        keys = new DataEncryptionKeyService(keyRepository, new DataEncryptionKeyGenerator(),
                new KeyEncryptionService(properties, new AesGcmEnvelopeCodec()));
        assignments = new KeyAssignmentService(assignmentRepository);
        management = new CryptoKeyManagementService(keys, assignments);
    }

    @Test
    void generatesWrappedKeysAssignsReassignsAndListsDeterministically() {
        AssignmentId second = id("ffffffff-ffff-ffff-ffff-ffffffffffff", "b");
        AssignmentId first = id("00000000-0000-0000-0000-000000000001", "a");
        AssignmentChange generated = management.generateAndAssign(second);
        UUID unassigned = keys.generateAndStore();
        AssignmentChange reassigned = management.assignExisting(first, generated.getKeyId());

        assertThat(generated.isGenerated()).isTrue();
        assertThat(reassigned.isGenerated()).isFalse();
        assertThat(keyRepository.keys.get(generated.getKeyId()).getWrappedKeyEnvelope())
                .doesNotContain("test-kek-never-persisted");
        assertThat(management.list().getTopics().keySet()).containsExactly(
                first.getTopicId(), second.getTopicId());
        assertThat(management.list().getUnassignedKeyIds()).containsExactly(unassigned);

        UUID replacement = keys.generateAndStore();
        AssignmentChange changed = management.assignExisting(first, replacement);
        assertThat(changed.getReplacedKeyId()).isEqualTo(generated.getKeyId());
        assertThat(assignmentRepository.index.get(generated.getKeyId())).doesNotContain(first);
        assertThat(assignmentRepository.index.get(replacement)).contains(first);
    }

    @Test
    void deletesOnlyAssignmentsThatStillPointAtTheIndexedKey() {
        AssignmentId live = id(UUID.randomUUID().toString(), "a");
        AssignmentId stale = id(UUID.randomUUID().toString(), "b");
        UUID deletedKey = keys.generateAndStore();
        UUID retainedKey = keys.generateAndStore();
        assignments.assign(live, deletedKey);
        assignments.assign(stale, retainedKey);
        assignmentRepository.index.computeIfAbsent(deletedKey, ignored -> new LinkedHashSet<>()).add(stale);

        DeletedKey deleted = management.delete(deletedKey);

        assertThat(deleted.getRemovedAssignments()).containsExactly(live);
        assertThat(keyRepository.exists(deletedKey)).isFalse();
        assertThat(assignments.find(live)).isEmpty();
        assertThat(assignments.find(stale)).get().extracting(KeyAssignment::getKeyId).isEqualTo(retainedKey);
        assertThat(assignmentRepository.index).doesNotContainKey(deletedKey);
    }

    @Test
    void resolverReadsFreshStateEveryTimeAndGetOrCreatePersistsAssignment() {
        AssignedKeyResolver resolver = new AssignedKeyResolver(assignments, keys);
        AssignmentId id = id(UUID.randomUUID().toString(), "a");
        UUID first = keys.generateAndStore();
        UUID second = keys.generateAndStore();
        assignments.assign(id, first);

        try (AssignedKey resolved = resolver.resolve(id)) {
            assertThat(resolved.getKeyId()).isEqualTo(first);
        }
        assignments.assign(id, second);
        try (AssignedKey resolved = resolver.resolve(id)) {
            assertThat(resolved.getKeyId()).isEqualTo(second);
        }

        AssignmentId absent = id(UUID.randomUUID().toString(), "c");
        try (AssignedKey generated = resolver.getOrCreate(absent)) {
            assertThat(assignments.find(absent)).get().extracting(KeyAssignment::getKeyId)
                    .isEqualTo(generated.getKeyId());
        }
    }

    @Test
    void rejectsMissingExistingKeyAndDanglingAssignments() {
        AssignmentId id = id(UUID.randomUUID().toString(), "a");
        assertThatThrownBy(() -> management.assignExisting(id, UUID.randomUUID()))
                .isInstanceOfSatisfying(BackendException.class, exception ->
                        assertThat(exception.getErrorCode()).isEqualTo(BackendErrorCode.KEY_NOT_FOUND));
        assignmentRepository.assign(id, UUID.randomUUID());
        assertThatThrownBy(management::list)
                .isInstanceOfSatisfying(BackendException.class, exception ->
                        assertThat(exception.getErrorCode()).isEqualTo(BackendErrorCode.KEY_MATERIAL_NOT_FOUND));
    }

    private AssignmentId id(String topicId, String suffix) {
        return new AssignmentId(UUID.fromString(topicId), suffix.repeat(64));
    }

    private static class MemoryKeyRepository implements CryptoKeyRepository {
        private final Map<UUID, CryptoKeyEntity> keys = new HashMap<>();
        @Override public void save(CryptoKeyEntity key) { keys.put(key.getKeyId(), key); }
        @Override public Optional<CryptoKeyEntity> findById(UUID keyId) { return Optional.ofNullable(keys.get(keyId)); }
        @Override public boolean exists(UUID keyId) { return keys.containsKey(keyId); }
        @Override public boolean delete(UUID keyId) { return keys.remove(keyId) != null; }
        @Override public Set<UUID> findAllKeyIds() { return new LinkedHashSet<>(keys.keySet()); }
    }

    private static class MemoryAssignmentRepository implements KeyAssignmentRepository {
        private final Map<AssignmentId, UUID> assignments = new HashMap<>();
        private final Map<UUID, Set<AssignmentId>> index = new HashMap<>();
        @Override public Optional<KeyAssignment> find(AssignmentId id) {
            return Optional.ofNullable(assignments.get(id)).map(keyId -> new KeyAssignment(id, keyId));
        }
        @Override public Optional<UUID> assign(AssignmentId id, UUID keyId) {
            UUID previous = assignments.put(id, keyId);
            if (previous != null && !previous.equals(keyId)) {
                index.getOrDefault(previous, Set.of()).remove(id);
            }
            index.computeIfAbsent(keyId, ignored -> new LinkedHashSet<>()).add(id);
            return Optional.ofNullable(previous);
        }
        @Override public void deleteIfAssignedTo(AssignmentId id, UUID keyId) {
            if (keyId.equals(assignments.get(id))) assignments.remove(id);
            index.getOrDefault(keyId, Set.of()).remove(id);
        }
        @Override public List<KeyAssignment> findAll() {
            List<KeyAssignment> result = new ArrayList<>();
            assignments.forEach((id, keyId) -> result.add(new KeyAssignment(id, keyId)));
            return result;
        }
        @Override public List<AssignmentId> findIndexedAssignments(UUID keyId) {
            return index.getOrDefault(keyId, Set.of()).stream().sorted().toList();
        }
        @Override public void deleteIndex(UUID keyId) { index.remove(keyId); }
    }
}
