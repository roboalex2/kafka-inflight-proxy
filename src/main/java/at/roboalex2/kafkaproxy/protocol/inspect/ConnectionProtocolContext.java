package at.roboalex2.kafkaproxy.protocol.inspect;

import at.roboalex2.kafkaproxy.logging.ConnectionLogWriter;
import at.roboalex2.kafkaproxy.protocol.codec.ParsedProtocolMessage;
import at.roboalex2.kafkaproxy.protocol.codec.ProtocolParser;
import at.roboalex2.kafkaproxy.protocol.correlation.ConnectionRequestRegistry;
import at.roboalex2.kafkaproxy.protocol.correlation.RequestContext;
import at.roboalex2.kafkaproxy.protocol.mapping.ProtocolModelMapper;
import at.roboalex2.kafkaproxy.protocol.serialization.KafkaProtocolMessageSerializer;
import at.roboalex2.kafkaproxy.protocol.topic.TopicIdentityResolver;
import at.roboalex2.kafkaproxy.protocol.transform.FetchResponseDecryptionTransformer;
import at.roboalex2.kafkaproxy.protocol.transform.MessageTransformationResult;
import at.roboalex2.kafkaproxy.protocol.transform.MetadataEndpointTransformer;
import at.roboalex2.kafkaproxy.protocol.transform.MissingBrokerMappingException;
import at.roboalex2.kafkaproxy.protocol.transform.MetadataTransformationException;
import at.roboalex2.kafkaproxy.protocol.transform.ProduceRequestEncryptionTransformer;
import at.roboalex2.kafkaproxy.protocol.transform.ProtocolTransformationException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/** All mutable protocol-inspection state for exactly one paired connection. */
public class ConnectionProtocolContext implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionProtocolContext.class);
    private final String connectionId;
    private final ProtocolParser protocolParser;
    private final ConnectionRequestRegistry requests = new ConnectionRequestRegistry();
    private final ConnectionLogWriter logWriter;
    private final ObjectMapper objectMapper;
    private final OrderedTaskExecutor executor;
    private final MetadataEndpointTransformer metadataTransformer;
    private final ProduceRequestEncryptionTransformer produceTransformer;
    private final FetchResponseDecryptionTransformer fetchTransformer;
    private final TopicIdentityResolver topicIdentityResolver;
    private final KafkaProtocolMessageSerializer serializer;
    private final ProtocolModelMapper modelMapper;
    private final AtomicLong messageCounter = new AtomicLong();
    private volatile boolean closing;

    public ConnectionProtocolContext(String connectionId, ProtocolParser protocolParser,
                                     ConnectionLogWriter logWriter, ObjectMapper objectMapper,
                                     TransformationExecutor inspectionExecutor, int perConnectionQueueCapacity,
                                     MetadataEndpointTransformer metadataTransformer,
                                     ProduceRequestEncryptionTransformer produceTransformer,
                                     FetchResponseDecryptionTransformer fetchTransformer,
                                     TopicIdentityResolver topicIdentityResolver,
                                     KafkaProtocolMessageSerializer serializer, ProtocolModelMapper modelMapper) {
        this.connectionId = connectionId;
        this.protocolParser = protocolParser;
        this.logWriter = logWriter;
        this.objectMapper = objectMapper;
        this.executor = new OrderedTaskExecutor(inspectionExecutor, perConnectionQueueCapacity);
        this.metadataTransformer = metadataTransformer;
        this.produceTransformer = produceTransformer;
        this.fetchTransformer = fetchTransformer;
        this.topicIdentityResolver = topicIdentityResolver;
        this.serializer = serializer;
        this.modelMapper = modelMapper;
    }

    /** Takes ownership of a client frame and completes with the safe frame to forward. */
    public synchronized boolean processClientFrame(ByteBuf completeFrame, Consumer<ByteBuf> onReady,
                                                   Runnable onFatalTransformationFailure,
                                                   Runnable onTaskCompleted) {
        if (closing) {
            ReferenceCountUtil.release(completeFrame);
            return true;
        }
        long number = messageCounter.incrementAndGet();
        boolean accepted = executor.execute(() -> {
            try {
                processClientFrame(number, completeFrame, onReady, onFatalTransformationFailure);
            } finally {
                onTaskCompleted.run();
            }
        });
        if (!accepted) ReferenceCountUtil.release(completeFrame);
        return accepted;
    }

    private void processClientFrame(long number, ByteBuf frame, Consumer<ByteBuf> onReady,
                                    Runnable onFatalTransformationFailure) {
        if (frame.readableBytes() < Integer.BYTES) {
            logUnknown(number, TrafficDirection.CLIENT_TO_BROKER, frame.readableBytes(), null);
            onReady.accept(frame);
            return;
        }
        ByteBuffer body = frame.nioBuffer(frame.readerIndex() + Integer.BYTES,
                frame.readableBytes() - Integer.BYTES);
        try {
            onReady.accept(inspectAndTransformRequest(number, body, frame));
        } catch (RuntimeException exception) {
            if (isSupportedProduceFrame(frame)) {
                logTransformationFailure(number, TrafficDirection.CLIENT_TO_BROKER, "ProduceRequest",
                        "Produce request could not be encrypted safely");
                ReferenceCountUtil.release(frame);
                onFatalTransformationFailure.run();
            } else {
                LOGGER.debug("Could not inspect Kafka request on connection {}", connectionId, exception);
                logUnknown(number, TrafficDirection.CLIENT_TO_BROKER,
                        Math.max(0, frame.readableBytes() - Integer.BYTES),
                        correlationId(frame, TrafficDirection.CLIENT_TO_BROKER));
                onReady.accept(frame);
            }
        }
    }

    private ByteBuf inspectAndTransformRequest(long number, ByteBuffer body, ByteBuf originalFrame) {
        int frameSize = body.remaining();
        ParsedProtocolMessage parsed = protocolParser.parseRequest(body);
        if ("Unknown".equals(parsed.getApiName())) {
            logUnknown(number, TrafficDirection.CLIENT_TO_BROKER, frameSize, parsed.getCorrelationId());
            return originalFrame;
        }
        RequestContext request = new RequestContext(connectionId, parsed.getCorrelationId(), parsed.getApiKey(),
                parsed.getApiName(), parsed.getApiVersion(), parsed.getHeaderVersion(),
                protocolParser.responseHeaderVersion(parsed.getApiKey(), parsed.getApiVersion()),
                parsed.isExpectsResponse(), number, Instant.now());
        if (request.isExpectsResponse() && !registerIfOpen(request)) {
            logWriter.append(number + " C -> B: Duplicate correlationId "
                    + request.getCorrelationId() + " for " + request.getApiName() + "Request");
        }
        logParsed(number, TrafficDirection.CLIENT_TO_BROKER, parsed, "ORIGINAL");
        if (parsed.getApiKey() != ApiKeys.PRODUCE.id || !parsed.isSupportedBody()
                || !(parsed.getMessageData() instanceof ProduceRequestData produceRequest)) {
            return originalFrame;
        }

        MessageTransformationResult result = produceTransformer.transform(produceRequest);
        if (!result.changed()) return originalFrame;
        ApiMessage transformed = result.message();
        ParsedProtocolMessage modified = modified(parsed, transformed,
                modelMapper.mapRequest(parsed.getApiKey(), parsed.getApiVersion(), transformed));
        ByteBuffer serialized = serializer.serializeRequest(parsed.getHeaderData(), parsed.getHeaderVersion(),
                transformed, parsed.getApiVersion());
        logParsed(number, TrafficDirection.CLIENT_TO_BROKER, modified, "FORWARDED_ENCRYPTED");
        ByteBuf outbound = Unpooled.wrappedBuffer(serialized);
        ReferenceCountUtil.release(originalFrame);
        return outbound;
    }

    /** Takes ownership of a broker frame and completes with the frame that may be forwarded. */
    public synchronized boolean processBrokerFrame(ByteBuf completeFrame, Consumer<ByteBuf> onReady,
                                                   Runnable onFatalTransformationFailure,
                                                   Runnable onTaskCompleted) {
        if (closing) {
            ReferenceCountUtil.release(completeFrame);
            return true;
        }
        long number = messageCounter.incrementAndGet();
        boolean accepted = executor.execute(() -> {
            try {
                processBrokerFrame(number, completeFrame, onReady, onFatalTransformationFailure);
            } finally {
                onTaskCompleted.run();
            }
        });
        if (!accepted) ReferenceCountUtil.release(completeFrame);
        return accepted;
    }

    private void processBrokerFrame(long number, ByteBuf frame, Consumer<ByteBuf> onReady,
                                    Runnable onFatalTransformationFailure) {
        if (frame.readableBytes() < Integer.BYTES) {
            logUnknown(number, TrafficDirection.BROKER_TO_CLIENT, frame.readableBytes(), null);
            onReady.accept(frame);
            return;
        }
        ByteBuffer body = frame.nioBuffer(frame.readerIndex() + Integer.BYTES,
                frame.readableBytes() - Integer.BYTES);
        try {
            ByteBuf outbound = inspectAndTransformResponse(number, body, frame);
            if (outbound != null) {
                onReady.accept(outbound);
            }
        } catch (MissingBrokerMappingException | MetadataTransformationException
                 | ProtocolTransformationException exception) {
            String messageType = exception instanceof ProtocolTransformationException
                    ? "FetchResponse" : "MetadataResponse";
            logTransformationFailure(number, TrafficDirection.BROKER_TO_CLIENT, messageType, exception.getMessage());
            ReferenceCountUtil.release(frame);
            onFatalTransformationFailure.run();
        } catch (RuntimeException exception) {
            LOGGER.debug("Could not inspect Kafka response on connection {}", connectionId, exception);
            logUnknown(number, TrafficDirection.BROKER_TO_CLIENT,
                    Math.max(0, frame.readableBytes() - Integer.BYTES),
                    correlationId(frame, TrafficDirection.BROKER_TO_CLIENT));
            onReady.accept(frame);
        }
    }

    private ByteBuf inspectAndTransformResponse(long number, ByteBuffer body, ByteBuf originalFrame) {
        int frameSize = body.remaining();
        if (body.remaining() < Integer.BYTES) {
            logUnknown(number, TrafficDirection.BROKER_TO_CLIENT, frameSize, null);
            return originalFrame;
        }
        int correlationId = body.getInt(body.position());
        RequestContext request = requests.remove(correlationId);
        if (request == null) {
            logUnknown(number, TrafficDirection.BROKER_TO_CLIENT, frameSize, correlationId);
            return originalFrame;
        }

        try {
            ParsedProtocolMessage parsed = protocolParser.parseResponse(body, request);
            String originalLabel = isSupportedFetch(request)
                    ? "ORIGINAL_ENCRYPTED" : "ORIGINAL";
            logParsed(number, TrafficDirection.BROKER_TO_CLIENT, parsed, originalLabel);
            if (!parsed.isSupportedBody()) return originalFrame;

            ApiMessage transformed;
            String forwardedLabel;
            if (parsed.getApiKey() == ApiKeys.METADATA.id
                    && parsed.getMessageData() instanceof MetadataResponseData metadata) {
                topicIdentityResolver.observe(metadata);
                transformed = metadataTransformer.transform(metadata, parsed.getApiVersion());
                forwardedLabel = "FORWARDED_MODIFIED";
            } else if (isSupportedFetch(request)
                    && parsed.getMessageData() instanceof FetchResponseData fetchResponse) {
                MessageTransformationResult result = fetchTransformer.transform(connectionId, fetchResponse);
                if (!result.changed()) return originalFrame;
                transformed = result.message();
                forwardedLabel = "FORWARDED_DECRYPTED";
            } else {
                return originalFrame;
            }

            ParsedProtocolMessage modified = modified(parsed, transformed,
                    modelMapper.mapResponse(parsed.getApiKey(), parsed.getApiVersion(), transformed));
            ByteBuffer serialized = serializer.serializeResponse(parsed.getHeaderData(), parsed.getHeaderVersion(),
                    transformed, parsed.getApiVersion());
            logParsed(number, TrafficDirection.BROKER_TO_CLIENT, modified, forwardedLabel);
            ByteBuf outbound = Unpooled.wrappedBuffer(serialized);
            ReferenceCountUtil.release(originalFrame);
            return outbound;
        } catch (MissingBrokerMappingException exception) {
            throw exception;
        } catch (RuntimeException exception) {
            if (request.getApiKey() == ApiKeys.METADATA.id) {
                throw new MetadataTransformationException("Metadata response could not be transformed safely",
                        exception);
            }
            if (isSupportedFetch(request)) {
                throw new ProtocolTransformationException("Fetch response could not be transformed safely",
                        exception);
            }
            throw exception;
        }
    }

    private void logParsed(long number, TrafficDirection direction, ParsedProtocolMessage parsed, String label) {
        if (!logWriter.isEnabled()) {
            return;
        }
        String heading = number + " " + direction.getLabel() + ": " + parsed.getApiName()
                + direction.getMessageType() + " Version: " + parsed.getApiVersion();
        if (parsed.getModel() == null) {
            logWriter.append(heading + (parsed.isSupportedBody() ? "" : " (body inspection unsupported)"));
            return;
        }
        try {
            String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(parsed.getModel());
            logWriter.append(heading + " " + label + System.lineSeparator() + json);
        } catch (JacksonException exception) {
            LOGGER.warn("Could not serialize protocol model on connection {}", connectionId, exception);
            logWriter.append(heading + " (JSON serialization failed)");
        }
    }

    private void logTransformationFailure(long number, TrafficDirection direction, String messageType,
                                          String message) {
        LOGGER.error("Closing Kafka connection {}: {}", connectionId, message);
        if (logWriter.isEnabled()) {
            logWriter.append(number + " " + direction.getLabel() + ": " + messageType
                    + " transformation failed: " + message);
        }
    }

    private ParsedProtocolMessage modified(ParsedProtocolMessage parsed, ApiMessage transformed,
                                           at.roboalex2.kafkaproxy.protocol.model.ProtocolMessageModel model) {
        return new ParsedProtocolMessage(parsed.getCorrelationId(), parsed.getApiKey(), parsed.getApiName(),
                parsed.getApiVersion(), parsed.getHeaderVersion(), true, parsed.isExpectsResponse(), model,
                parsed.getHeaderData(), transformed);
    }

    private boolean isSupportedProduceFrame(ByteBuf frame) {
        int offset = frame.readerIndex() + Integer.BYTES;
        return frame.writerIndex() - offset >= 2 * Short.BYTES
                && frame.getShort(offset) == ApiKeys.PRODUCE.id
                && frame.getShort(offset + Short.BYTES) >= 3
                && frame.getShort(offset + Short.BYTES) <= 13;
    }

    private boolean isSupportedFetch(RequestContext request) {
        return request.getApiKey() == ApiKeys.FETCH.id
                && request.getApiVersion() >= 4 && request.getApiVersion() <= 18;
    }

    private synchronized boolean registerIfOpen(RequestContext request) {
        return closing || requests.register(request);
    }

    private void logUnknown(long number, TrafficDirection direction, int size, Integer correlationId) {
        if (!logWriter.isEnabled()) {
            return;
        }
        String correlation = correlationId == null ? "" : ", correlationId: " + correlationId;
        logWriter.append(number + " " + direction.getLabel() + ": Unknown Kafka frame (size: "
                + size + " bytes" + correlation + ")");
    }

    private Integer correlationId(ByteBuf frame, TrafficDirection direction) {
        int offset = frame.readerIndex() + Integer.BYTES + (direction == TrafficDirection.CLIENT_TO_BROKER ? 4 : 0);
        return frame.writerIndex() - offset >= Integer.BYTES ? frame.getInt(offset) : null;
    }

    public int pendingRequestCount() { return requests.size(); }
    public long messageCount() { return messageCounter.get(); }
    public boolean isTransformationQueueAtCapacity() { return executor.isAtCapacity(); }

    @Override
    public synchronized void close() {
        if (closing) {
            return;
        }
        closing = true;
        requests.clear();
        if (!executor.execute(logWriter::close)) logWriter.close();
    }
}
