package at.roboalex2.kafkaproxy.keystore.repository;

import at.roboalex2.kafkaproxy.keystore.model.CryptoKeyEntity;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public interface CryptoKeyRepository {
    void save(CryptoKeyEntity key);
    Optional<CryptoKeyEntity> findById(UUID keyId);
    boolean exists(UUID keyId);
    boolean delete(UUID keyId);
    Set<UUID> findAllKeyIds();
}
