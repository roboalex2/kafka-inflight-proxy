package at.roboalex2.kafkaproxy.keystore.service;

import at.roboalex2.kafkaproxy.api.error.BackendErrorCode;
import at.roboalex2.kafkaproxy.api.error.BackendServiceException;
import at.roboalex2.kafkaproxy.crypto.key.DataEncryptionKeyGenerator;
import at.roboalex2.kafkaproxy.crypto.key.KeyEncryptionService;
import at.roboalex2.kafkaproxy.crypto.key.KeyMaterial;
import at.roboalex2.kafkaproxy.keystore.model.AssignedKey;
import at.roboalex2.kafkaproxy.keystore.model.CryptoKeyEntity;
import at.roboalex2.kafkaproxy.keystore.repository.CryptoKeyRepository;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class DataEncryptionKeyService {
    private final CryptoKeyRepository repository;
    private final DataEncryptionKeyGenerator generator;
    private final KeyEncryptionService encryptionService;

    public DataEncryptionKeyService(CryptoKeyRepository repository, DataEncryptionKeyGenerator generator,
                                    KeyEncryptionService encryptionService) {
        this.repository = repository;
        this.generator = generator;
        this.encryptionService = encryptionService;
    }

    public UUID generateAndStore() {
        try (KeyMaterial material = generator.generate()) {
            byte[] key = material.copyKeyBytes();
            try {
                repository.save(new CryptoKeyEntity(material.getKeyId(), encryptionService.wrap(material.getKeyId(), key)));
                return material.getKeyId();
            } finally {
                java.util.Arrays.fill(key, (byte) 0);
            }
        }
    }

    public AssignedKey generateAndStoreForImmediateUse() {
        try (KeyMaterial material = generator.generate()) {
            byte[] key = material.copyKeyBytes();
            repository.save(new CryptoKeyEntity(material.getKeyId(), encryptionService.wrap(material.getKeyId(), key)));
            try {
                return new AssignedKey(material.getKeyId(), key);
            } finally {
                java.util.Arrays.fill(key, (byte) 0);
            }
        }
    }

    public Optional<AssignedKey> resolve(UUID keyId) {
        return repository.findById(keyId).map(entity -> toAssignedKey(keyId, entity));
    }

    private AssignedKey toAssignedKey(UUID keyId, CryptoKeyEntity entity) {
        byte[] plaintext = encryptionService.unwrap(keyId, entity.getWrappedKeyEnvelope());
        try {
            return new AssignedKey(keyId, plaintext);
        } finally {
            java.util.Arrays.fill(plaintext, (byte) 0);
        }
    }
    public boolean exists(UUID keyId) { return repository.exists(keyId); }
    public Set<UUID> findAllKeyIds() { return repository.findAllKeyIds(); }
    public void deleteRequired(UUID keyId) {
        if (!repository.delete(keyId)) {
            throw new BackendServiceException(BackendErrorCode.KEY_NOT_FOUND,
                    "Cryptographic key " + keyId + " was not found");
        }
    }
}
