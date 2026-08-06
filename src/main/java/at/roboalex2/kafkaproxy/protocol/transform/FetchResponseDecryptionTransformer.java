package at.roboalex2.kafkaproxy.protocol.transform;

import at.roboalex2.kafkaproxy.config.KafkaProxyProperties;
import at.roboalex2.kafkaproxy.crypto.record.RecordFieldCryptography;
import at.roboalex2.kafkaproxy.keystore.model.AssignedKey;
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
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class FetchResponseDecryptionTransformer {
    private static final Logger LOGGER = LoggerFactory.getLogger(FetchResponseDecryptionTransformer.class);
    private final TopicIdentityResolver topicIdentityResolver;
    private final AssignedKeyResolver keyResolver;
    private final RecordFieldCryptography cryptography;
    private final RecordBatchTransformer batchTransformer;
    private final String keyHeader;
    private final String ivHeader;

    public FetchResponseDecryptionTransformer(TopicIdentityResolver topicIdentityResolver,
                                              AssignedKeyResolver keyResolver,
                                              RecordFieldCryptography cryptography,
                                              RecordBatchTransformer batchTransformer,
                                              KafkaProxyProperties properties) {
        this.topicIdentityResolver = topicIdentityResolver;
        this.keyResolver = keyResolver;
        this.cryptography = cryptography;
        this.batchTransformer = batchTransformer;
        this.keyHeader = properties.getCrypto().getEncryptionKeyHeaderName();
        this.ivHeader = properties.getCrypto().getEncryptionIvHeaderName();
    }

    public MessageTransformationResult transform(String connectionId, FetchResponseData original) {
        FetchResponseData transformed = original.duplicate();
        boolean changed = false;
        for (FetchResponseData.FetchableTopicResponse topic : transformed.responses()) {
            UUID directTopicId = toJavaUuid(topic.topicId());
            if (directTopicId != null) topicIdentityResolver.observe(topic.topic(), topic.topicId());
            for (FetchResponseData.PartitionData partition : topic.partitions()) {
                if (partition.records() == null) continue;
                RecordTransformationContext context = new RecordTransformationContext(
                        directTopicId, topic.topic(), partition.partitionIndex());
                RecordsTransformationResult result = batchTransformer.transform(partition.records(), context,
                        (record, recordContext) -> decryptRecord(connectionId, record, recordContext), false);
                if (result.changed()) {
                    partition.setRecords(result.records());
                    changed = true;
                }
            }
        }
        return new MessageTransformationResult(transformed, changed);
    }

    private RecordComponents decryptRecord(String connectionId, Record record,
                                           RecordTransformationContext context) {
        Header[] headers = record.headers();
        List<Header> keyHeaders = matching(headers, keyHeader);
        List<Header> ivHeaders = matching(headers, ivHeader);
        if (keyHeaders.isEmpty() && ivHeaders.isEmpty()) {
            return new RecordComponents(null, null, headers, false);
        }

        UUID keyId = null;
        try {
            if (keyHeaders.size() != 1 || ivHeaders.size() != 1) {
                throw new IllegalArgumentException("Reserved encryption headers are missing or duplicated");
            }
            keyId = parseCanonicalUuid(keyHeaders.getFirst().value());
            byte[] iv = parseCanonicalIv(ivHeaders.getFirst().value());
            if (context.topicId() == null) {
                topicIdentityResolver.resolveRequired(context.topicName(), Uuid.ZERO_UUID);
            }
            try (AssignedKey assignedKey = keyResolver.resolveKey(keyId)
                    .orElseThrow(() -> new IllegalStateException("Cryptographic key material is unavailable"))) {
                byte[] dek = assignedKey.copyKeyBytes();
                try {
                    byte[] plaintextKey = record.hasKey()
                            ? cryptography.decrypt(copy(record.key()), dek, iv, keyId) : null;
                    byte[] plaintextValue = record.hasValue()
                            ? cryptography.decrypt(copy(record.value()), dek, iv, keyId) : null;
                    Header[] plaintextHeaders = decryptHeaders(headers, dek, iv, keyId);
                    return new RecordComponents(plaintextKey, plaintextValue, plaintextHeaders, true);
                } finally {
                    Arrays.fill(dek, (byte) 0);
                }
            }
        } catch (RuntimeException exception) {
            LOGGER.warn("Substituting undecryptable Kafka record: connection={}, topic={}, partition={}, "
                            + "offset={}, keyId={}, failure={}", connectionId, safeTopic(context),
                    context.partition(), record.offset(), keyId, exception.getClass().getSimpleName());
            return new RecordComponents(null, null, reservedHeadersOnly(headers), true);
        }
    }

    private Header[] decryptHeaders(Header[] headers, byte[] dek, byte[] iv, UUID keyId) {
        List<Header> plaintext = new ArrayList<>(headers.length);
        for (Header header : headers) {
            if (isReserved(header)) {
                plaintext.add(new RecordHeader(header.key(), header.value()));
                continue;
            }
            String name = cryptography.decryptHeaderKey(header.key(), dek, iv, keyId);
            byte[] value = header.value() == null ? null : cryptography.decrypt(
                    Base64.getDecoder().decode(header.value()), dek, iv, keyId);
            plaintext.add(new RecordHeader(name, value));
        }
        return plaintext.toArray(Header[]::new);
    }

    private UUID parseCanonicalUuid(byte[] value) {
        if (value == null) throw new IllegalArgumentException("Encryption key header has no value");
        return UUID.fromString(new String(value, StandardCharsets.UTF_8));
    }

    private byte[] parseCanonicalIv(byte[] value) {
        if (value == null) throw new IllegalArgumentException("Encryption IV header has no value");
        String text = new String(value, StandardCharsets.UTF_8);
        byte[] iv = Base64.getUrlDecoder().decode(text);
        if (iv.length != RecordFieldCryptography.IV_BYTES) {
            throw new IllegalArgumentException("Encryption IV is malformed");
        }
        return iv;
    }

    private List<Header> matching(Header[] headers, String name) {
        List<Header> result = new ArrayList<>();
        for (Header header : headers) if (name.equals(header.key())) result.add(header);
        return result;
    }

    private Header[] reservedHeadersOnly(Header[] headers) {
        List<Header> reserved = new ArrayList<>();
        for (Header header : headers) {
            if (isReserved(header)) reserved.add(new RecordHeader(header.key(), header.value()));
        }
        return reserved.toArray(Header[]::new);
    }

    private boolean isReserved(Header header) {
        return keyHeader.equals(header.key()) || ivHeader.equals(header.key());
    }

    private byte[] copy(ByteBuffer buffer) {
        ByteBuffer duplicate = buffer.duplicate();
        byte[] copy = new byte[duplicate.remaining()];
        duplicate.get(copy);
        return copy;
    }

    private UUID toJavaUuid(Uuid topicId) {
        return topicId == null || Uuid.ZERO_UUID.equals(topicId) ? null
                : new UUID(topicId.getMostSignificantBits(), topicId.getLeastSignificantBits());
    }

    private String safeTopic(RecordTransformationContext context) {
        if (context.topicName() != null && !context.topicName().isBlank()) return context.topicName();
        if (context.topicId() != null) {
            return topicIdentityResolver.resolveName(context.topicId()).orElse(context.topicId().toString());
        }
        return "<unnamed>";
    }
}
