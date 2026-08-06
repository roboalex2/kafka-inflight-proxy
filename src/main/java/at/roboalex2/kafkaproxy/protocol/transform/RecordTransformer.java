package at.roboalex2.kafkaproxy.protocol.transform;

import org.apache.kafka.common.record.Record;

@FunctionalInterface
public interface RecordTransformer {
    RecordComponents transform(Record record, RecordTransformationContext context);
}
