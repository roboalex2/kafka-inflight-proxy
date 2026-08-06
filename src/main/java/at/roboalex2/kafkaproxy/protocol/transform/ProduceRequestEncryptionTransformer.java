package at.roboalex2.kafkaproxy.protocol.transform;

import at.roboalex2.kafkaproxy.config.KafkaProxyProperties;
import at.roboalex2.kafkaproxy.crypto.hash.RecordKeyHashService;
import at.roboalex2.kafkaproxy.crypto.record.RecordFieldCryptography;
import at.roboalex2.kafkaproxy.keystore.model.AssignedKey;
import at.roboalex2.kafkaproxy.keystore.model.AssignmentId;
import at.roboalex2.kafkaproxy.keystore.service.AssignedKeyResolver;
import at.roboalex2.kafkaproxy.protocol.topic.TopicIdentityResolver;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.record.Record;
import org.springframework.stereotype.Component;

@Component
public class ProduceRequestEncryptionTransformer {
    private final TopicIdentityResolver topicIdentityResolver;
    private final RecordKeyHashService hashService;
    private final AssignedKeyResolver keyResolver;
    private final RecordFieldCryptography cryptography;
    private final RecordBatchTransformer batchTransformer;
    private final String keyHeader;
    private final String ivHeader;

    public ProduceRequestEncryptionTransformer(TopicIdentityResolver topicIdentityResolver,
                                               RecordKeyHashService hashService,
                                               AssignedKeyResolver keyResolver,
                                               RecordFieldCryptography cryptography,
                                               RecordBatchTransformer batchTransformer,
                                               KafkaProxyProperties properties) {
        this.topicIdentityResolver = topicIdentityResolver;
        this.hashService = hashService;
        this.keyResolver = keyResolver;
        this.cryptography = cryptography;
        this.batchTransformer = batchTransformer;
        this.keyHeader = properties.getCrypto().getEncryptionKeyHeaderName();
        this.ivHeader = properties.getCrypto().getEncryptionIvHeaderName();
    }

    public MessageTransformationResult transform(ProduceRequestData original) {
        ProduceRequestData transformed = original.duplicate();
        boolean changed = false;
        for (ProduceRequestData.TopicProduceData topic : transformed.topicData()) {
            UUID topicId = toJavaUuid(topic.topicId());
            if (topicId != null) topicIdentityResolver.observe(topic.name(), topic.topicId());
            for (ProduceRequestData.PartitionProduceData partition : topic.partitionData()) {
                if (partition.records() == null) continue;
                RecordTransformationContext context = new RecordTransformationContext(
                        topicId, topic.name(), partition.index());
                RecordsTransformationResult result = batchTransformer.transform(partition.records(), context,
                        this::encryptRecord, true);
                if (result.changed()) {
                    partition.setRecords(result.records());
                    changed = true;
                }
            }
        }
        return new MessageTransformationResult(transformed, changed);
    }

    private RecordComponents encryptRecord(Record record, RecordTransformationContext context) {
        UUID topicId = context.topicId() != null ? context.topicId()
                : topicIdentityResolver.resolveRequired(context.topicName(), Uuid.ZERO_UUID);
        byte[] plaintextKey = copy(record.key());
        AssignmentId assignmentId = new AssignmentId(topicId, hashService.hash(plaintextKey));
        try (AssignedKey assignedKey = keyResolver.getOrCreate(assignmentId)) {
            byte[] dek = assignedKey.copyKeyBytes();
            try {
                byte[] iv = cryptography.newIv();
                byte[] encryptedKey = plaintextKey == null ? null
                        : cryptography.encrypt(plaintextKey, dek, iv, assignedKey.getKeyId());
                byte[] plaintextValue = copy(record.value());
                byte[] encryptedValue = plaintextValue == null ? null
                        : cryptography.encrypt(plaintextValue, dek, iv, assignedKey.getKeyId());
                Header[] encryptedHeaders = encryptHeaders(record.headers(), dek, iv, assignedKey.getKeyId());
                return new RecordComponents(encryptedKey, encryptedValue, encryptedHeaders, true);
            } finally {
                Arrays.fill(dek, (byte) 0);
            }
        }
    }

    private Header[] encryptHeaders(Header[] headers, byte[] dek, byte[] iv, UUID keyId) {
        List<Header> encrypted = new ArrayList<>(headers.length + 2);
        for (Header header : headers) {
            if (isReserved(header.key())) continue;
            String encryptedName = cryptography.encryptHeaderKey(header.key(), dek, iv, keyId);
            byte[] encryptedValue = header.value() == null ? null : Base64.getEncoder().encode(
                    cryptography.encrypt(header.value(), dek, iv, keyId));
            encrypted.add(new RecordHeader(encryptedName, encryptedValue));
        }
        encrypted.add(new RecordHeader(keyHeader, keyId.toString().getBytes(StandardCharsets.UTF_8)));
        encrypted.add(new RecordHeader(ivHeader,
                Base64.getUrlEncoder().withoutPadding().encodeToString(iv).getBytes(StandardCharsets.UTF_8)));
        return encrypted.toArray(Header[]::new);
    }

    private boolean isReserved(String headerName) {
        return keyHeader.equals(headerName) || ivHeader.equals(headerName);
    }

    private byte[] copy(ByteBuffer buffer) {
        if (buffer == null) return null;
        ByteBuffer duplicate = buffer.duplicate();
        byte[] copy = new byte[duplicate.remaining()];
        duplicate.get(copy);
        return copy;
    }

    private UUID toJavaUuid(Uuid topicId) {
        return topicId == null || Uuid.ZERO_UUID.equals(topicId) ? null
                : new UUID(topicId.getMostSignificantBits(), topicId.getLeastSignificantBits());
    }
}
