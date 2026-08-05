package at.roboalex2.kafkaproxy.api.controller;

import at.roboalex2.kafkaproxy.api.error.BackendErrorCode;
import at.roboalex2.kafkaproxy.api.error.BackendServiceException;
import at.roboalex2.kafkaproxy.api.generated.CryptoKeyManagementApi;
import at.roboalex2.kafkaproxy.api.generated.model.AssignmentListResponse;
import at.roboalex2.kafkaproxy.api.generated.model.AssignmentMode;
import at.roboalex2.kafkaproxy.api.generated.model.AssignmentRequest;
import at.roboalex2.kafkaproxy.api.generated.model.AssignmentResponse;
import at.roboalex2.kafkaproxy.api.generated.model.DeleteKeyResponse;
import at.roboalex2.kafkaproxy.api.mapper.CryptoApiMapper;
import at.roboalex2.kafkaproxy.crypto.hash.RecordKeyHashService;
import at.roboalex2.kafkaproxy.keystore.model.AssignmentId;
import at.roboalex2.kafkaproxy.keystore.service.CryptoKeyManagementService;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class CryptoKeyManagementController implements CryptoKeyManagementApi {
    private final CryptoKeyManagementService service;
    private final RecordKeyHashService hashService;
    private final CryptoApiMapper mapper;

    public CryptoKeyManagementController(CryptoKeyManagementService service, RecordKeyHashService hashService,
                                         CryptoApiMapper mapper) {
        this.service = service;
        this.hashService = hashService;
        this.mapper = mapper;
    }

    @Override public ResponseEntity<AssignmentListResponse> listAssignments() {
        return ResponseEntity.ok(mapper.toApi(service.list()));
    }

    @Override
    public ResponseEntity<AssignmentResponse> putAssignment(UUID topicId, String recordKeyHash,
                                                            AssignmentRequest request) {
        AssignmentId id = new AssignmentId(topicId, hashService.normalizeAndValidate(recordKeyHash));
        validateRequest(request);
        if (request.getMode() == AssignmentMode.ASSIGN_EXISTING) {
            return ResponseEntity.ok(mapper.toApi(service.assignExisting(id, request.getKeyId())));
        }
        return ResponseEntity.ok(mapper.toApi(service.generateAndAssign(id)));
    }

    @Override public ResponseEntity<DeleteKeyResponse> deleteKey(UUID keyId) {
        return ResponseEntity.ok(mapper.toApi(service.delete(keyId)));
    }

    private void validateRequest(AssignmentRequest request) {
        boolean existingIsValid = request.getMode() == AssignmentMode.ASSIGN_EXISTING && request.getKeyId() != null;
        boolean generationIsValid = request.getMode() == AssignmentMode.GENERATE_NEW && request.getKeyId() == null;
        if (!existingIsValid && !generationIsValid) {
            throw new BackendServiceException(BackendErrorCode.INVALID_ASSIGNMENT_REQUEST,
                    "ASSIGN_EXISTING requires keyId; GENERATE_NEW forbids keyId");
        }
    }
}
