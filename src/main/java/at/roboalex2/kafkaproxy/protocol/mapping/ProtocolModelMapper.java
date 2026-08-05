package at.roboalex2.kafkaproxy.protocol.mapping;

import at.roboalex2.kafkaproxy.protocol.model.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.DefaultRecord;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.springframework.stereotype.Component;

/** Maps Kafka's versioned generated messages into proxy-owned, JSON-safe object graphs. */
@Component
public class ProtocolModelMapper {
    public ProtocolMessageModel mapRequest(short apiKey, short version, Message message) {
        Map<String, Object> fields = messageFields(message);
        return switch (apiKey) {
            case 0 -> new ProduceRequestModel(version, fields);
            case 1 -> new FetchRequestModel(version, fields);
            case 3 -> new MetadataRequestModel(version, fields);
            default -> throw new IllegalArgumentException("No detailed request model for API " + apiKey);
        };
    }

    public ProtocolMessageModel mapResponse(short apiKey, short version, Message message) {
        Map<String, Object> fields = messageFields(message);
        return switch (apiKey) {
            case 0 -> new ProduceResponseModel(version, fields);
            case 1 -> new FetchResponseModel(version, fields);
            case 3 -> new MetadataResponseModel(version, fields);
            default -> throw new IllegalArgumentException("No detailed response model for API " + apiKey);
        };
    }

    private Map<String, Object> messageFields(Message message) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (Field field : message.getClass().getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            try {
                String accessorName = field.getName().startsWith("_")
                        ? field.getName().substring(1) : field.getName();
                Method accessor = message.getClass().getMethod(accessorName);
                result.put(accessorName, mapValue(accessor.invoke(message)));
            } catch (ReflectiveOperationException exception) {
                throw new IllegalStateException("Cannot map Kafka field " + field.getName(), exception);
            }
        }
        return result;
    }

    private Object mapValue(Object value) {
        if (value == null || value instanceof String || value instanceof Number || value instanceof Boolean) {
            return value;
        }
        if (value instanceof Uuid uuid) {
            return new UUID(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        }
        if (value instanceof ByteBuffer bytes) {
            return copy(bytes);
        }
        if (value instanceof byte[] bytes) {
            return bytes.clone();
        }
        if (value instanceof RawTaggedField taggedField) {
            return new TaggedField(taggedField.tag(), taggedField.data());
        }
        if (value instanceof BaseRecords records) {
            return mapRecords(records);
        }
        if (value instanceof Message message) {
            return messageFields(message);
        }
        if (value instanceof Collection<?> collection) {
            return collection.stream().map(this::mapValue).toList();
        }
        if (value instanceof Iterable<?> iterable) {
            List<Object> values = new ArrayList<>();
            iterable.forEach(item -> values.add(mapValue(item)));
            return values;
        }
        return value.toString();
    }

    public List<RecordBatchModel> mapRecords(BaseRecords records) {
        if (!(records instanceof MemoryRecords memoryRecords)) {
            return List.of();
        }
        List<RecordBatchModel> batches = new ArrayList<>();
        for (MutableRecordBatch batch : memoryRecords.batches()) {
            if (batch.magic() != 2 || !(batch instanceof DefaultRecordBatch defaultBatch)) {
                continue;
            }
            List<RecordModel> mappedRecords = new ArrayList<>();
            for (Record record : defaultBatch) {
                mappedRecords.add(mapRecord(record, defaultBatch));
            }
            short attributes = readAttributes(defaultBatch);
            batches.add(new RecordBatchModel(defaultBatch.baseOffset(), defaultBatch.sizeInBytes() - 12,
                    defaultBatch.partitionLeaderEpoch(), defaultBatch.magic(), defaultBatch.checksum(), attributes,
                    defaultBatch.compressionType().name(), defaultBatch.timestampType().name(),
                    defaultBatch.isTransactional(), defaultBatch.isControlBatch(),
                    defaultBatch.deleteHorizonMs().isPresent(),
                    Math.toIntExact(defaultBatch.lastOffset() - defaultBatch.baseOffset()),
                    defaultBatch.baseTimestamp(), defaultBatch.maxTimestamp(), defaultBatch.producerId(),
                    defaultBatch.producerEpoch(), defaultBatch.baseSequence(),
                    defaultBatch.countOrNull() == null ? mappedRecords.size() : defaultBatch.countOrNull(),
                    mappedRecords));
        }
        return batches;
    }

    private RecordModel mapRecord(Record record, DefaultRecordBatch batch) {
        byte attributes = record instanceof DefaultRecord defaultRecord ? defaultRecord.attributes() : 0;
        List<RecordHeaderModel> headers = new ArrayList<>();
        for (Header header : record.headers()) {
            byte[] value = header.value();
            headers.add(new RecordHeaderModel(header.key().getBytes(StandardCharsets.UTF_8).length,
                    header.key(), value == null ? -1 : value.length, value));
        }
        return new RecordModel(encodedBodyLength(record.sizeInBytes()), attributes,
                record.timestamp() - batch.baseTimestamp(), record.offset() - batch.baseOffset(),
                record.keySize(), copy(record.key()), record.valueSize(), copy(record.value()), headers);
    }

    private short readAttributes(DefaultRecordBatch batch) {
        ByteBuffer bytes = ByteBuffer.allocate(batch.sizeInBytes());
        batch.writeTo(bytes);
        return bytes.getShort(21);
    }

    private int encodedBodyLength(int totalEncodedLength) {
        for (int prefixLength = 1; prefixLength <= 5; prefixLength++) {
            int bodyLength = totalEncodedLength - prefixLength;
            if (bodyLength >= 0 && varintSize(bodyLength) == prefixLength) {
                return bodyLength;
            }
        }
        return totalEncodedLength;
    }

    private int varintSize(int value) {
        int zigZag = (value << 1) ^ (value >> 31);
        int size = 1;
        while ((zigZag & ~0x7f) != 0) {
            size++;
            zigZag >>>= 7;
        }
        return size;
    }

    private byte[] copy(ByteBuffer source) {
        if (source == null) {
            return null;
        }
        ByteBuffer duplicate = source.duplicate();
        byte[] result = new byte[duplicate.remaining()];
        duplicate.get(result);
        return result;
    }
}
