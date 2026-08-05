package at.roboalex2.kafkaproxy.api.mapper;

import at.roboalex2.kafkaproxy.api.generated.model.AssignmentListResponse;
import at.roboalex2.kafkaproxy.api.generated.model.AssignmentResponse;
import at.roboalex2.kafkaproxy.api.generated.model.DeleteKeyResponse;
import at.roboalex2.kafkaproxy.api.generated.model.RemovedAssignment;
import at.roboalex2.kafkaproxy.keystore.model.AssignmentChange;
import at.roboalex2.kafkaproxy.keystore.model.AssignmentOverview;
import at.roboalex2.kafkaproxy.keystore.model.DeletedKey;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class CryptoApiMapper {
    public AssignmentListResponse toApi(AssignmentOverview overview) {
        Map<String, Map<String, UUID>> topics = new LinkedHashMap<>();
        overview.getTopics().forEach((topicId, hashes) ->
                topics.put(topicId.toString(), new LinkedHashMap<>(hashes)));
        return new AssignmentListResponse(topics, overview.getUnassignedKeyIds());
    }

    public AssignmentResponse toApi(AssignmentChange change) {
        return new AssignmentResponse(change.getAssignmentId().getTopicId(),
                change.getAssignmentId().getRecordKeyHash(), change.getKeyId(), change.isGenerated())
                .replacedKeyId(change.getReplacedKeyId());
    }

    public DeleteKeyResponse toApi(DeletedKey deleted) {
        return new DeleteKeyResponse(deleted.getKeyId(), true, deleted.getRemovedAssignments().stream()
                .map(id -> new RemovedAssignment(id.getTopicId(), id.getRecordKeyHash())).toList());
    }
}
