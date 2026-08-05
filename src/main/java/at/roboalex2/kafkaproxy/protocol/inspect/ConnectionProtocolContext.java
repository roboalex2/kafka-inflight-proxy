package at.roboalex2.kafkaproxy.protocol.inspect;

import at.roboalex2.kafkaproxy.logging.ConnectionLogWriter;
import at.roboalex2.kafkaproxy.protocol.codec.ParsedProtocolMessage;
import at.roboalex2.kafkaproxy.protocol.codec.ProtocolCodecRegistry;
import at.roboalex2.kafkaproxy.protocol.correlation.ConnectionRequestRegistry;
import at.roboalex2.kafkaproxy.protocol.correlation.RequestContext;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/** All mutable protocol-inspection state for exactly one paired connection. */
public class ConnectionProtocolContext implements KafkaRequestInspector, KafkaResponseInspector, AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionProtocolContext.class);
    private final String connectionId;
    private final ProtocolCodecRegistry codecs;
    private final ConnectionRequestRegistry requests = new ConnectionRequestRegistry();
    private final ConnectionLogWriter logWriter;
    private final ObjectMapper objectMapper;
    private final OrderedConnectionExecutor executor;
    private final AtomicLong messageCounter = new AtomicLong();
    private volatile boolean closing;

    public ConnectionProtocolContext(String connectionId, ProtocolCodecRegistry codecs,
                                     ConnectionLogWriter logWriter, ObjectMapper objectMapper,
                                     ProtocolInspectionExecutor inspectionExecutor) {
        this.connectionId = connectionId;
        this.codecs = codecs;
        this.logWriter = logWriter;
        this.objectMapper = objectMapper;
        this.executor = new OrderedConnectionExecutor(inspectionExecutor);
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
        ParsedProtocolMessage parsed = codecs.parseRequest(frameBody);
        if ("Unknown".equals(parsed.getApiName())) {
            logUnknown(messageNumber, TrafficDirection.CLIENT_TO_BROKER, frameSize, parsed.getCorrelationId());
            return;
        }
        RequestContext request = new RequestContext(connectionId, parsed.getCorrelationId(), parsed.getApiKey(),
                parsed.getApiName(), parsed.getApiVersion(), parsed.getHeaderVersion(),
                codecs.responseHeaderVersion(parsed.getApiKey(), parsed.getApiVersion()),
                parsed.isExpectsResponse(), messageNumber, Instant.now());
        if (request.isExpectsResponse() && !registerIfOpen(request)) {
            logWriter.append(messageNumber + " C -> B: Duplicate correlationId "
                    + request.getCorrelationId() + " for " + request.getApiName() + "Request");
        }
        logParsed(messageNumber, TrafficDirection.CLIENT_TO_BROKER, parsed);
    }

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
        logParsed(messageNumber, TrafficDirection.BROKER_TO_CLIENT, codecs.parseResponse(frameBody, request));
    }

    private void logParsed(long number, TrafficDirection direction, ParsedProtocolMessage parsed) {
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
            logWriter.append(heading + " ORIGINAL" + System.lineSeparator() + json);
        } catch (JacksonException exception) {
            LOGGER.warn("Could not serialize protocol model on connection {}", connectionId, exception);
            logWriter.append(heading + " (JSON serialization failed)");
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
