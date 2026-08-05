package at.roboalex2.kafkaproxy.keystore.repository;

import static org.assertj.core.api.Assertions.assertThat;

import at.roboalex2.kafkaproxy.config.KafkaProxyProperties;
import at.roboalex2.kafkaproxy.config.RedisConfiguration;
import at.roboalex2.kafkaproxy.crypto.envelope.AesGcmEnvelopeCodec;
import at.roboalex2.kafkaproxy.crypto.key.DataEncryptionKeyGenerator;
import at.roboalex2.kafkaproxy.crypto.key.KeyEncryptionService;
import at.roboalex2.kafkaproxy.keystore.model.AssignmentId;
import at.roboalex2.kafkaproxy.keystore.model.CryptoKeyEntity;
import at.roboalex2.kafkaproxy.keystore.redis.RedisKeyNames;
import at.roboalex2.kafkaproxy.keystore.service.CryptoKeyManagementService;
import at.roboalex2.kafkaproxy.keystore.service.DataEncryptionKeyService;
import at.roboalex2.kafkaproxy.keystore.service.KeyAssignmentService;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers(disabledWithoutDocker = true)
class RedisRepositoriesIntegrationTest {
    @Container
    private static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:7.4-alpine")).withExposedPorts(6379);

    private LettuceConnectionFactory connectionFactory;
    private StringRedisTemplate redis;
    private RedisCryptoKeyRepository keyRepository;
    private RedisKeyAssignmentRepository assignmentRepository;

    @BeforeEach
    void setUp() {
        KafkaProxyProperties properties = properties();
        RedisConfiguration configuration = new RedisConfiguration();
        connectionFactory = configuration.redisConnectionFactory(properties);
        connectionFactory.afterPropertiesSet();
        connectionFactory.start();
        redis = configuration.stringRedisTemplate(connectionFactory);
        redis.afterPropertiesSet();
        keyRepository = new RedisCryptoKeyRepository(redis);
        assignmentRepository = new RedisKeyAssignmentRepository(redis);
    }

    @AfterEach
    void tearDown() {
        if (connectionFactory != null) connectionFactory.destroy();
    }

    @Test
    void storesWrappedKeysAssignmentsAndReverseIndexesInRealRedis() {
        UUID firstKey = UUID.randomUUID();
        UUID secondKey = UUID.randomUUID();
        AssignmentId id = new AssignmentId(UUID.randomUUID(), "ab".repeat(32));
        byte[] plaintext = "0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8);
        assertThat(plaintext).hasSize(32);
        String wrapped = encryptionService().wrap(firstKey, plaintext);
        keyRepository.save(new CryptoKeyEntity(firstKey, wrapped));
        keyRepository.save(new CryptoKeyEntity(secondKey, encryptionService().wrap(secondKey, new byte[32])));

        assertThat(keyRepository.findById(firstKey)).get()
                .extracting(CryptoKeyEntity::getWrappedKeyEnvelope).isEqualTo(wrapped);
        assertThat(redis.opsForHash().get(RedisKeyNames.KEYS_HASH, firstKey.toString()).toString())
                .doesNotContain(Base64.getEncoder().encodeToString(plaintext));

        assertThat(assignmentRepository.assign(id, firstKey)).isEmpty();
        assertThat(assignmentRepository.findIndexedAssignments(firstKey)).containsExactly(id);
        assertThat(assignmentRepository.assign(id, secondKey)).contains(firstKey);
        assertThat(assignmentRepository.findIndexedAssignments(firstKey)).doesNotContain(id);
        assertThat(assignmentRepository.findIndexedAssignments(secondKey)).containsExactly(id);
    }

    @Test
    void deletionHonorsCurrentAssignmentDespiteStaleReverseIndex() {
        DataEncryptionKeyService keys = new DataEncryptionKeyService(keyRepository,
                new DataEncryptionKeyGenerator(), encryptionService());
        KeyAssignmentService assignments = new KeyAssignmentService(assignmentRepository);
        CryptoKeyManagementService management = new CryptoKeyManagementService(keys, assignments);
        AssignmentId live = new AssignmentId(UUID.randomUUID(), "01".repeat(32));
        AssignmentId stale = new AssignmentId(UUID.randomUUID(), "02".repeat(32));
        UUID deletedKey = keys.generateAndStore();
        UUID retainedKey = keys.generateAndStore();
        assignments.assign(live, deletedKey);
        assignments.assign(stale, retainedKey);
        redis.opsForSet().add(RedisKeyNames.assignmentIndex(deletedKey), RedisKeyNames.indexMember(stale));

        assertThat(management.delete(deletedKey).getRemovedAssignments()).containsExactly(live);
        assertThat(assignments.find(live)).isEmpty();
        assertThat(assignments.find(stale)).get().extracting(value -> value.getKeyId()).isEqualTo(retainedKey);
        assertThat(keyRepository.exists(retainedKey)).isTrue();
        assertThat(redis.hasKey(RedisKeyNames.assignmentIndex(deletedKey))).isFalse();
    }

    private KeyEncryptionService encryptionService() {
        return new KeyEncryptionService(properties(), new AesGcmEnvelopeCodec());
    }

    private KafkaProxyProperties properties() {
        KafkaProxyProperties properties = new KafkaProxyProperties();
        properties.getRedis().setHost(REDIS.getHost());
        properties.getRedis().setPort(REDIS.getMappedPort(6379));
        properties.getRedis().setConnectTimeout(Duration.ofSeconds(5));
        properties.getRedis().setCommandTimeout(Duration.ofSeconds(5));
        properties.getCrypto().setKeyEncryptionKey("integration-test-kek");
        return properties;
    }
}
