package at.roboalex2.kafkaproxy.keystore.repository;

import at.roboalex2.kafkaproxy.api.error.BackendErrorCode;
import at.roboalex2.kafkaproxy.api.error.BackendServiceException;
import at.roboalex2.kafkaproxy.keystore.model.CryptoKeyEntity;
import at.roboalex2.kafkaproxy.keystore.redis.RedisKeyNames;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class RedisCryptoKeyRepository implements CryptoKeyRepository {
    private final StringRedisTemplate redis;

    public RedisCryptoKeyRepository(StringRedisTemplate redis) { this.redis = redis; }

    @Override public void save(CryptoKeyEntity key) {
        execute(() -> redis.opsForHash().put(RedisKeyNames.KEYS_HASH,
                key.getKeyId().toString(), key.getWrappedKeyEnvelope()));
    }

    @Override public Optional<CryptoKeyEntity> findById(UUID keyId) {
        String value = executeWithResult(() -> (String) redis.opsForHash()
                .get(RedisKeyNames.KEYS_HASH, keyId.toString()));
        return Optional.ofNullable(value).map(envelope -> new CryptoKeyEntity(keyId, envelope));
    }

    @Override public boolean exists(UUID keyId) {
        return Boolean.TRUE.equals(executeWithResult(() -> redis.opsForHash()
                .hasKey(RedisKeyNames.KEYS_HASH, keyId.toString())));
    }

    @Override public boolean delete(UUID keyId) {
        Long removed = executeWithResult(() -> redis.opsForHash()
                .delete(RedisKeyNames.KEYS_HASH, keyId.toString()));
        return removed != null && removed > 0;
    }

    @Override public Set<UUID> findAllKeyIds() {
        Map<Object, Object> entries = executeWithResult(() -> redis.opsForHash().entries(RedisKeyNames.KEYS_HASH));
        Set<UUID> ids = new LinkedHashSet<>();
        entries.keySet().forEach(value -> ids.add(UUID.fromString(value.toString())));
        return ids;
    }

    private void execute(Runnable operation) {
        try { operation.run(); } catch (DataAccessException exception) { throw redisFailure(exception); }
    }
    private <T> T executeWithResult(ResultOperation<T> operation) {
        try { return operation.execute(); } catch (DataAccessException exception) { throw redisFailure(exception); }
    }
    private BackendServiceException redisFailure(Exception cause) {
        return new BackendServiceException(BackendErrorCode.REDIS_OPERATION_FAILED,
                "Redis key-store operation failed", cause);
    }
    private interface ResultOperation<T> { T execute(); }
}
