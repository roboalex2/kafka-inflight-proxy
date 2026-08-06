package at.roboalex2.kafkaproxy.keystore.repository;

import at.roboalex2.kafkaproxy.api.error.BackendErrorCode;
import at.roboalex2.kafkaproxy.api.error.BackendServiceException;
import at.roboalex2.kafkaproxy.keystore.model.AssignmentId;
import at.roboalex2.kafkaproxy.keystore.model.KeyAssignment;
import at.roboalex2.kafkaproxy.keystore.redis.RedisKeyNames;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class RedisKeyAssignmentRepository implements KeyAssignmentRepository {
    private final StringRedisTemplate redis;

    public RedisKeyAssignmentRepository(StringRedisTemplate redis) { this.redis = redis; }

    @Override public Optional<KeyAssignment> find(AssignmentId id) {
        String value = execute(() -> (String) redis.opsForHash()
                .get(RedisKeyNames.assignments(id.getTopicId()), id.getRecordKeyHash()));
        return Optional.ofNullable(value).map(UUID::fromString).map(keyId -> new KeyAssignment(id, keyId));
    }

    @Override public Optional<UUID> assign(AssignmentId id, UUID keyId) {
        Optional<UUID> previous = find(id).map(KeyAssignment::getKeyId);
        String member = RedisKeyNames.indexMember(id);
        previous.filter(old -> !old.equals(keyId)).ifPresent(old -> execute(() ->
                redis.opsForSet().remove(RedisKeyNames.assignmentIndex(old), member)));
        execute(() -> {
            redis.opsForHash().put(RedisKeyNames.assignments(id.getTopicId()),
                    id.getRecordKeyHash(), keyId.toString());
            return null;
        });
        execute(() -> redis.opsForSet().add(RedisKeyNames.assignmentIndex(keyId), member));
        return previous;
    }

    @Override public void deleteIfAssignedTo(AssignmentId id, UUID keyId) {
        Optional<KeyAssignment> current = find(id);
        if (current.isPresent() && current.get().getKeyId().equals(keyId)) {
            execute(() -> redis.opsForHash().delete(RedisKeyNames.assignments(id.getTopicId()),
                    id.getRecordKeyHash()));
        }
        execute(() -> redis.opsForSet().remove(RedisKeyNames.assignmentIndex(keyId), RedisKeyNames.indexMember(id)));
    }

    @Override public List<KeyAssignment> findAll() {
        Set<String> keys = execute(() -> redis.keys(RedisKeyNames.ASSIGNMENTS_PREFIX + "*"));
        List<KeyAssignment> assignments = new ArrayList<>();
        if (keys == null) return assignments;
        keys.stream().sorted().forEach(key -> {
            UUID topicId = UUID.fromString(key.substring(RedisKeyNames.ASSIGNMENTS_PREFIX.length()));
            Map<Object, Object> entries = execute(() -> redis.opsForHash().entries(key));
            entries.forEach((hash, keyId) -> assignments.add(new KeyAssignment(
                    new AssignmentId(topicId, hash.toString()), UUID.fromString(keyId.toString()))));
        });
        assignments.sort(Comparator.comparing(KeyAssignment::getAssignmentId));
        return assignments;
    }

    @Override public List<AssignmentId> findIndexedAssignments(UUID keyId) {
        Set<String> members = execute(() -> redis.opsForSet().members(RedisKeyNames.assignmentIndex(keyId)));
        if (members == null) return List.of();
        return members.stream().map(RedisKeyNames::parseIndexMember).sorted().toList();
    }

    @Override public void deleteIndex(UUID keyId) {
        execute(() -> redis.delete(RedisKeyNames.assignmentIndex(keyId)));
    }

    private <T> T execute(Supplier<T> operation) {
        try { return operation.get(); } catch (DataAccessException exception) {
            throw new BackendServiceException(BackendErrorCode.REDIS_OPERATION_FAILED,
                    "Redis assignment operation failed", exception);
        }
    }
}
