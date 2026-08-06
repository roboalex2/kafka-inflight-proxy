package at.roboalex2.kafkaproxy.protocol.transform;

import at.roboalex2.kafkaproxy.api.error.BackendErrorCode;
import at.roboalex2.kafkaproxy.api.error.BackendServiceException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.DefaultRecord;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.springframework.stereotype.Component;

@Component
public class RecordBatchTransformer {
    public RecordsTransformationResult transform(BaseRecords source, RecordTransformationContext baseContext,
                                                  RecordTransformer recordTransformer,
                                                  boolean rejectUnsupportedBatch) {
        if (!(source instanceof MemoryRecords memoryRecords)) {
            if (rejectUnsupportedBatch) throw unsupported("Kafka records are not memory-backed");
            return new RecordsTransformationResult(null, false);
        }
        List<ByteBuffer> encodedBatches = new ArrayList<>();
        boolean anyChanged = false;
        for (MutableRecordBatch mutableBatch : memoryRecords.batches()) {
            if (!(mutableBatch instanceof DefaultRecordBatch batch) || batch.magic() != RecordBatch.MAGIC_VALUE_V2) {
                if (rejectUnsupportedBatch) throw unsupported("Only Kafka magic-2 record batches can be transformed");
                encodedBatches.add(copyBatch(mutableBatch));
                continue;
            }
            if (batch.isControlBatch()) {
                encodedBatches.add(copyBatch(batch));
                continue;
            }
            List<OriginalAndTransformedRecord> records = new ArrayList<>();
            boolean batchChanged = false;
            for (Record record : batch) {
                RecordTransformationContext context = new RecordTransformationContext(
                        baseContext.topicId(), baseContext.topicName(), baseContext.partition());
                RecordComponents transformed = recordTransformer.transform(record, context);
                if (transformed.changed() && record instanceof DefaultRecord defaultRecord
                        && defaultRecord.attributes() != 0) {
                    throw unsupported("Non-zero record attributes cannot be preserved safely");
                }
                records.add(new OriginalAndTransformedRecord(record, transformed));
                batchChanged |= transformed.changed();
            }
            if (batchChanged) {
                encodedBatches.add(rebuildBatch(batch, records));
                anyChanged = true;
            } else {
                encodedBatches.add(copyBatch(batch));
            }
        }
        if (!anyChanged) return new RecordsTransformationResult(memoryRecords, false);
        int size = encodedBatches.stream().mapToInt(ByteBuffer::remaining).sum();
        ByteBuffer combined = ByteBuffer.allocate(size);
        encodedBatches.forEach(batch -> combined.put(batch.duplicate()));
        combined.flip();
        return new RecordsTransformationResult(MemoryRecords.readableRecords(combined), true);
    }

    private ByteBuffer rebuildBatch(DefaultRecordBatch batch, List<OriginalAndTransformedRecord> records) {
        int initialCapacity = Math.max(1024, batch.sizeInBytes() * 2);
        long logAppendTime = batch.timestampType() == org.apache.kafka.common.record.TimestampType.LOG_APPEND_TIME
                ? batch.maxTimestamp() : RecordBatch.NO_TIMESTAMP;
        long deleteHorizon = batch.deleteHorizonMs().orElse(RecordBatch.NO_TIMESTAMP);
        ByteBufferOutputStream output = new ByteBufferOutputStream(initialCapacity);
        try (MemoryRecordsBuilder builder = new MemoryRecordsBuilder(output, batch.magic(),
                Compression.of(batch.compressionType()).build(), batch.timestampType(), batch.baseOffset(),
                logAppendTime, batch.producerId(), batch.producerEpoch(), batch.baseSequence(),
                batch.isTransactional(), batch.isControlBatch(), batch.partitionLeaderEpoch(),
                Integer.MAX_VALUE, deleteHorizon)) {
            for (OriginalAndTransformedRecord item : records) {
                Record original = item.original();
                RecordComponents transformed = item.transformed();
                if (transformed.changed()) {
                    builder.appendWithOffset(original.offset(), original.timestamp(), buffer(transformed.key()),
                            buffer(transformed.value()), copyHeaders(transformed.headers()));
                } else {
                    builder.appendWithOffset(original.offset(), original.timestamp(), duplicate(original.key()),
                            duplicate(original.value()), copyHeaders(original.headers()));
                }
            }
            MemoryRecords rebuilt = builder.build();
            ByteBuffer bytes = rebuilt.buffer().duplicate();
            ByteBuffer copy = ByteBuffer.allocate(bytes.remaining()).put(bytes);
            copy.flip();
            return copy;
        }
    }

    private ByteBuffer copyBatch(MutableRecordBatch batch) {
        ByteBuffer copy = ByteBuffer.allocate(batch.sizeInBytes());
        batch.writeTo(copy);
        return copy.flip();
    }

    private ByteBuffer buffer(byte[] value) { return value == null ? null : ByteBuffer.wrap(value); }
    private ByteBuffer duplicate(ByteBuffer value) { return value == null ? null : value.duplicate(); }
    private Header[] copyHeaders(Header[] headers) { return headers == null ? Record.EMPTY_HEADERS : headers.clone(); }
    private BackendServiceException unsupported(String message) {
        return new BackendServiceException(BackendErrorCode.PROTOCOL_TRANSFORMATION_FAILED, message);
    }

    private record OriginalAndTransformedRecord(Record original, RecordComponents transformed) { }
}
