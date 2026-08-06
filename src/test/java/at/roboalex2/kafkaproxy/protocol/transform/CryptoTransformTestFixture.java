package at.roboalex2.kafkaproxy.protocol.transform;

import at.roboalex2.kafkaproxy.api.error.BackendErrorCode;
import at.roboalex2.kafkaproxy.api.error.BackendServiceException;
import at.roboalex2.kafkaproxy.config.KafkaProxyProperties;
import at.roboalex2.kafkaproxy.crypto.envelope.AesGcmEnvelopeCodec;
import at.roboalex2.kafkaproxy.crypto.hash.RecordKeyHashService;
import at.roboalex2.kafkaproxy.crypto.key.DataEncryptionKeyGenerator;
import at.roboalex2.kafkaproxy.crypto.key.KeyEncryptionService;
import at.roboalex2.kafkaproxy.crypto.record.RecordFieldCryptography;
import at.roboalex2.kafkaproxy.keystore.model.AssignmentId;
import at.roboalex2.kafkaproxy.keystore.model.CryptoKeyEntity;
import at.roboalex2.kafkaproxy.keystore.model.KeyAssignment;
import at.roboalex2.kafkaproxy.keystore.repository.CryptoKeyRepository;
import at.roboalex2.kafkaproxy.keystore.repository.KeyAssignmentRepository;
import at.roboalex2.kafkaproxy.keystore.service.AssignedKeyResolver;
import at.roboalex2.kafkaproxy.keystore.service.CryptoKeyManagementService;
import at.roboalex2.kafkaproxy.keystore.service.DataEncryptionKeyService;
import at.roboalex2.kafkaproxy.keystore.service.KeyAssignmentService;
import at.roboalex2.kafkaproxy.protocol.topic.TopicIdentityResolver;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public final class CryptoTransformTestFixture {
    public final KafkaProxyProperties properties = new KafkaProxyProperties();
    public final MemoryKeyRepository keyRepository = new MemoryKeyRepository();
    public final MemoryAssignmentRepository assignmentRepository = new MemoryAssignmentRepository();
    public final TopicIdentityResolver topicIdentityResolver = new TopicIdentityResolver();
    public final DataEncryptionKeyService keyService;
    public final KeyAssignmentService assignmentService;
    public final CryptoKeyManagementService managementService;
    public final ProduceRequestEncryptionTransformer produceTransformer;
    public final FetchResponseDecryptionTransformer fetchTransformer;

    public CryptoTransformTestFixture() {
        properties.getCrypto().setKeyEncryptionKey("transform-test-kek");
        keyService = new DataEncryptionKeyService(keyRepository, new DataEncryptionKeyGenerator(),
                new KeyEncryptionService(properties, new AesGcmEnvelopeCodec()));
        assignmentService = new KeyAssignmentService(assignmentRepository);
        managementService = new CryptoKeyManagementService(keyService, assignmentService);
        AssignedKeyResolver resolver = new AssignedKeyResolver(assignmentService, keyService);
        RecordFieldCryptography cryptography = new RecordFieldCryptography();
        RecordBatchTransformer batches = new RecordBatchTransformer();
        produceTransformer = new ProduceRequestEncryptionTransformer(topicIdentityResolver,
                new RecordKeyHashService(), resolver, cryptography, batches, properties);
        fetchTransformer = new FetchResponseDecryptionTransformer(
                topicIdentityResolver, resolver, cryptography, batches, properties);
    }

    public static final class MemoryKeyRepository implements CryptoKeyRepository {
        public final Map<UUID, CryptoKeyEntity> keys = new HashMap<>();
        public volatile boolean failWrites;
        @Override public void save(CryptoKeyEntity key) {
            if (failWrites) {
                throw new BackendServiceException(BackendErrorCode.REDIS_OPERATION_FAILED,
                        "Simulated Redis write failure");
            }
            keys.put(key.getKeyId(), key);
        }
        @Override public Optional<CryptoKeyEntity> findById(UUID keyId) { return Optional.ofNullable(keys.get(keyId)); }
        @Override public boolean exists(UUID keyId) { return keys.containsKey(keyId); }
        @Override public boolean delete(UUID keyId) { return keys.remove(keyId) != null; }
        @Override public Set<UUID> findAllKeyIds() { return new LinkedHashSet<>(keys.keySet()); }
    }

    public static final class MemoryAssignmentRepository implements KeyAssignmentRepository {
        public final Map<AssignmentId, UUID> assignments = new HashMap<>();
        public final Map<UUID, Set<AssignmentId>> index = new HashMap<>();
        @Override public Optional<KeyAssignment> find(AssignmentId id) {
            return Optional.ofNullable(assignments.get(id)).map(keyId -> new KeyAssignment(id, keyId));
        }
        @Override public Optional<UUID> assign(AssignmentId id, UUID keyId) {
            UUID previous = assignments.put(id, keyId);
            if (previous != null && !previous.equals(keyId)) {
                Set<AssignmentId> old = index.get(previous);
                if (old != null) old.remove(id);
            }
            index.computeIfAbsent(keyId, ignored -> new LinkedHashSet<>()).add(id);
            return Optional.ofNullable(previous);
        }
        @Override public void deleteIfAssignedTo(AssignmentId id, UUID keyId) {
            if (keyId.equals(assignments.get(id))) assignments.remove(id);
            Set<AssignmentId> indexed = index.get(keyId);
            if (indexed != null) indexed.remove(id);
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
