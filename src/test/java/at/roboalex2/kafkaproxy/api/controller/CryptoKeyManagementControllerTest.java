package at.roboalex2.kafkaproxy.api.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import at.roboalex2.kafkaproxy.api.error.BackendErrorCode;
import at.roboalex2.kafkaproxy.api.error.BackendServiceException;
import at.roboalex2.kafkaproxy.api.error.GlobalExceptionHandler;
import at.roboalex2.kafkaproxy.api.mapper.CryptoApiMapper;
import at.roboalex2.kafkaproxy.config.KafkaProxyProperties;
import at.roboalex2.kafkaproxy.crypto.hash.RecordKeyHashService;
import at.roboalex2.kafkaproxy.keystore.model.AssignmentChange;
import at.roboalex2.kafkaproxy.keystore.model.AssignmentId;
import at.roboalex2.kafkaproxy.keystore.model.AssignmentOverview;
import at.roboalex2.kafkaproxy.keystore.model.DeletedKey;
import at.roboalex2.kafkaproxy.keystore.service.CryptoKeyManagementService;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class CryptoKeyManagementControllerTest {
    private static final UUID TOPIC_ID = UUID.fromString("10000000-0000-0000-0000-000000000000");
    private static final UUID KEY_ID = UUID.fromString("20000000-0000-0000-0000-000000000000");
    private static final String HASH = "ab".repeat(32);
    private CryptoKeyManagementService service;
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        service = org.mockito.Mockito.mock(CryptoKeyManagementService.class);
        KafkaProxyProperties properties = new KafkaProxyProperties();
        properties.getCrypto().setKeyEncryptionKey("rest-secret-to-redact");
        properties.getRedis().setPassword("redis-secret-to-redact");
        CryptoKeyManagementController controller = new CryptoKeyManagementController(service,
                new RecordKeyHashService(), new CryptoApiMapper());
        mvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new GlobalExceptionHandler(properties)).build();
    }

    @Test
    void listsAssignmentsAsJson() throws Exception {
        Map<UUID, Map<String, UUID>> topics = new LinkedHashMap<>();
        topics.put(TOPIC_ID, Map.of(HASH, KEY_ID));
        when(service.list()).thenReturn(new AssignmentOverview(topics, List.of(UUID.fromString(
                "30000000-0000-0000-0000-000000000000"))));

        mvc.perform(get("/api/v1/crypto/assignments"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.topics['" + TOPIC_ID + "']['" + HASH + "']").value(KEY_ID.toString()))
                .andExpect(jsonPath("$.unassignedKeyIds[0]").value("30000000-0000-0000-0000-000000000000"));
    }

    @Test
    void assignsExistingKeyAndNormalizesHash() throws Exception {
        AssignmentId id = new AssignmentId(TOPIC_ID, HASH);
        when(service.assignExisting(any(), any())).thenReturn(new AssignmentChange(id, KEY_ID, false, null));

        mvc.perform(put("/api/v1/crypto/assignments/{topicId}/{hash}", TOPIC_ID, HASH.toUpperCase())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"mode\":\"ASSIGN_EXISTING\",\"keyId\":\"" + KEY_ID + "\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.recordKeyHash").value(HASH))
                .andExpect(jsonPath("$.keyId").value(KEY_ID.toString()))
                .andExpect(jsonPath("$.generated").value(false));

        ArgumentCaptor<AssignmentId> assignment = ArgumentCaptor.forClass(AssignmentId.class);
        verify(service).assignExisting(assignment.capture(), org.mockito.ArgumentMatchers.eq(KEY_ID));
        org.assertj.core.api.Assertions.assertThat(assignment.getValue()).isEqualTo(id);
    }

    @Test
    void generatesAssignmentAndDeletesKey() throws Exception {
        AssignmentId id = new AssignmentId(TOPIC_ID, HASH);
        when(service.generateAndAssign(any())).thenReturn(new AssignmentChange(id, KEY_ID, true, null));
        when(service.delete(KEY_ID)).thenReturn(new DeletedKey(KEY_ID, List.of(id)));

        mvc.perform(put("/api/v1/crypto/assignments/{topicId}/{hash}", TOPIC_ID, HASH)
                        .contentType(MediaType.APPLICATION_JSON).content("{\"mode\":\"GENERATE_NEW\"}"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.generated").value(true));
        mvc.perform(delete("/api/v1/crypto/keys/{keyId}", KEY_ID))
                .andExpect(status().isOk()).andExpect(jsonPath("$.deleted").value(true))
                .andExpect(jsonPath("$.removedAssignments[0].topicId").value(TOPIC_ID.toString()));
    }

    @Test
    void rejectsInvalidModeCombinationsHashesAndUuidsWithStructuredErrors() throws Exception {
        mvc.perform(put("/api/v1/crypto/assignments/{topicId}/{hash}", TOPIC_ID, HASH)
                        .contentType(MediaType.APPLICATION_JSON).content("{\"mode\":\"ASSIGN_EXISTING\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_ASSIGNMENT_REQUEST"))
                .andExpect(jsonPath("$.cause").isNotEmpty());
        mvc.perform(put("/api/v1/crypto/assignments/{topicId}/{hash}", TOPIC_ID, "bad")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"mode\":\"GENERATE_NEW\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_RECORD_KEY_HASH"));
        mvc.perform(put("/api/v1/crypto/assignments/{topicId}/{hash}", "not-a-uuid", HASH)
                        .contentType(MediaType.APPLICATION_JSON).content("{\"mode\":\"GENERATE_NEW\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_TOPIC_ID"));
    }

    @Test
    void mapsBackendFailuresAndRedactsConfiguredSecrets() throws Exception {
        when(service.delete(KEY_ID)).thenThrow(new BackendServiceException(BackendErrorCode.REDIS_OPERATION_FAILED,
                "Redis failed using redis-secret-to-redact and rest-secret-to-redact"));

        mvc.perform(delete("/api/v1/crypto/keys/{keyId}", KEY_ID))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.code").value("REDIS_OPERATION_FAILED"))
                .andExpect(jsonPath("$.message").value("Redis failed using [REDACTED] and [REDACTED]"))
                .andExpect(jsonPath("$.cause").isNotEmpty());
    }

    @Test
    void mapsMissingKeysTo404AndUnknownFailuresTo500() throws Exception {
        when(service.delete(KEY_ID)).thenThrow(new BackendServiceException(BackendErrorCode.KEY_NOT_FOUND,
                "Key does not exist"));
        mvc.perform(delete("/api/v1/crypto/keys/{keyId}", KEY_ID))
                .andExpect(status().isNotFound()).andExpect(jsonPath("$.code").value("KEY_NOT_FOUND"));

        when(service.list()).thenThrow(new IllegalStateException("unexpected failure"));
        mvc.perform(get("/api/v1/crypto/assignments"))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.code").value("INTERNAL_SERVER_ERROR"))
                .andExpect(jsonPath("$.message").value("unexpected failure"));
    }
}
