package at.roboalex2.kafkaproxy.protocol.inspect;

import at.roboalex2.kafkaproxy.logging.ConnectionLogWriter;
import at.roboalex2.kafkaproxy.protocol.codec.ParsedProtocolMessage;
import at.roboalex2.kafkaproxy.protocol.codec.ProtocolParser;
import at.roboalex2.kafkaproxy.protocol.correlation.ConnectionRequestRegistry;
import at.roboalex2.kafkaproxy.protocol.correlation.RequestContext;
import at.roboalex2.kafkaproxy.protocol.mapping.ProtocolModelMapper;
import at.roboalex2.kafkaproxy.protocol.serialization.ProtocolMessageSerializer;
import at.roboalex2.kafkaproxy.protocol.transform.MessageTransformer;
import at.roboalex2.kafkaproxy.protocol.transform.MissingBrokerMappingException;
import at.roboalex2.kafkaproxy.protocol.transform.MetadataTransformationException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/** All mutable protocol-inspection state for exactly one paired connection. */
public class ConnectionProtocolContext implements KafkaRequestInspector, KafkaResponseInspector, AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionProtocolContext.class);
    private final String connectionId;
    private final ProtocolParser protocolParser;
    private final ConnectionRequestRegistry requests = new ConnectionRequestRegistry();
    private final ConnectionLogWriter logWriter;
    private final ObjectMapper objectMapper;
    private final OrderedTaskExecutor executor;
    private final MessageTransformer transformer;
    private final ProtocolMessageSerializer serializer;
    private final ProtocolModelMapper modelMapper;
    private final AtomicLong messageCounter = new AtomicLong();
    private volatile boolean closing;

    public ConnectionProtocolContext(String connectionId, ProtocolParser protocolParser,
                                     ConnectionLogWriter logWriter, ObjectMapper objectMapper,
                                     VirtualThreadExecutor inspectionExecutor, MessageTransformer transformer,
                                     ProtocolMessageSerializer serializer, ProtocolModelMapper modelMapper) {
        this.connectionId = connectionId;
        this.protocolParser = protocolParser;
        this.logWriter = logWriter;
        this.objectMapper = objectMapper;
        this.executor = new OrderedTaskExecutor(inspectionExecutor);
        this.transformer = transformer;
        this.serializer = serializer;
        this.modelMapper = modelMapper;
    }

    /** Takes ownership of a broker frame and completes with the frame that may be forwarded. */
    public synchronized void processBrokerFrame(ByteBuf completeFrame, Consumer<ByteBuf> onReady,
                                                Runnable onFatalTransformationFailure) {
        if (closing) {
            ReferenceCountUtil.release(completeFrame);
            return;
        }
        long number = messageCounter.incrementAndGet();
        executor.execute(() -> processBrokerFrame(number, completeFrame, onReady, onFatalTransformationFailure));
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
        } catch (MissingBrokerMappingException | MetadataTransformationException exception) {
            logTransformationFailure(number, exception.getMessage());
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

        ParsedProtocolMessage parsed = protocolParser.parseResponse(body, request);
        logParsed(number, TrafficDirection.BROKER_TO_CLIENT, parsed, "ORIGINAL");
        if (parsed.getApiKey() != ApiKeys.METADATA.id || !parsed.isSupportedBody()) {
            return originalFrame;
        }

        try {
            ApiMessage transformed = transformer.transform(parsed.getApiKey(), parsed.getApiVersion(),
                    parsed.getMessageData());
            ParsedProtocolMessage modified = new ParsedProtocolMessage(parsed.getCorrelationId(), parsed.getApiKey(),
                    parsed.getApiName(), parsed.getApiVersion(), parsed.getHeaderVersion(), true, true,
                    modelMapper.mapResponse(parsed.getApiKey(), parsed.getApiVersion(), transformed),
                    parsed.getHeaderData(), transformed);
            ByteBuffer serialized = serializer.serializeResponse(parsed.getHeaderData(), parsed.getHeaderVersion(),
                    transformed, parsed.getApiVersion());
            logParsed(number, TrafficDirection.BROKER_TO_CLIENT, modified, "FORWARDED_MODIFIED");
            ByteBuf outbound = Unpooled.wrappedBuffer(serialized);
            ReferenceCountUtil.release(originalFrame);
            return outbound;
        } catch (MissingBrokerMappingException exception) {
            throw exception;
        } catch (RuntimeException exception) {
            throw new MetadataTransformationException("Metadata response could not be transformed safely", exception);
        }
    }

    public synchronized void inspect(TrafficDirection direction, ByteBuf completeFrame) {
        if (closing) {
            return;
        }
        long number = messageCounter.incrementAndGet();
        ByteBuf retained = completeFrame.retainedDuplicate();
        executor.execute(() -> {
            try {
                if (retained.readableBytes() < Integer.BYTES) {
                    logUnknown(number, direction, retained.readableBytes(), null);
                    return;
                }
                ByteBuffer body = retained.nioBuffer(retained.readerIndex() + Integer.BYTES,
                        retained.readableBytes() - Integer.BYTES);
                if (direction == TrafficDirection.CLIENT_TO_BROKER) {
                    inspectRequest(number, body);
                } else {
                    // TODO: This can be removed since processBrokerFrame handels responses now.
                    inspectResponse(number, body);
                }
            } catch (RuntimeException exception) {
                LOGGER.debug("Could not inspect Kafka frame on connection {}", connectionId, exception);
                logUnknown(number, direction, Math.max(0, retained.readableBytes() - Integer.BYTES),
                        correlationId(retained, direction));
            } finally {
                ReferenceCountUtil.release(retained);
            }
        });
    }

    @Override
    public void inspectRequest(long messageNumber, ByteBuffer frameBody) {
        int frameSize = frameBody.remaining();
        ParsedProtocolMessage parsed = protocolParser.parseRequest(frameBody);
        if ("Unknown".equals(parsed.getApiName())) {
            logUnknown(messageNumber, TrafficDirection.CLIENT_TO_BROKER, frameSize, parsed.getCorrelationId());
            return;
        }
        RequestContext request = new RequestContext(connectionId, parsed.getCorrelationId(), parsed.getApiKey(),
                parsed.getApiName(), parsed.getApiVersion(), parsed.getHeaderVersion(),
                protocolParser.responseHeaderVersion(parsed.getApiKey(), parsed.getApiVersion()),
                parsed.isExpectsResponse(), messageNumber, Instant.now());
        if (request.isExpectsResponse() && !registerIfOpen(request)) {
            logWriter.append(messageNumber + " C -> B: Duplicate correlationId "
                    + request.getCorrelationId() + " for " + request.getApiName() + "Request");
        }
        logParsed(messageNumber, TrafficDirection.CLIENT_TO_BROKER, parsed, "ORIGINAL");
    }

    // TODO: This can be removed since processBrokerFrame handels responses now.
    @Override
    public void inspectResponse(long messageNumber, ByteBuffer frameBody) {
        int frameSize = frameBody.remaining();
        if (frameBody.remaining() < Integer.BYTES) {
            logUnknown(messageNumber, TrafficDirection.BROKER_TO_CLIENT, frameSize, null);
            return;
        }
        int correlationId = frameBody.getInt(frameBody.position());
        RequestContext request = requests.remove(correlationId);
        if (request == null) {
            logUnknown(messageNumber, TrafficDirection.BROKER_TO_CLIENT, frameSize, correlationId);
            return;
        }
        logParsed(messageNumber, TrafficDirection.BROKER_TO_CLIENT,
                protocolParser.parseResponse(frameBody, request), "ORIGINAL");
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

    private void logTransformationFailure(long number, String message) {
        LOGGER.error("Closing Kafka connection {}: {}", connectionId, message);
        if (logWriter.isEnabled()) {
            logWriter.append(number + " B -> C: MetadataResponse transformation failed: " + message);
        }
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

    @Override
    public synchronized void close() {
        if (closing) {
            return;
        }
        closing = true;
        requests.clear();
        executor.execute(() -> {
            logWriter.close();
        });
    }
}
