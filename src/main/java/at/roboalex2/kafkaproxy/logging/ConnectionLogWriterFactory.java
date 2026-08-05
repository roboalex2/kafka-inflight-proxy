package at.roboalex2.kafkaproxy.logging;

import at.roboalex2.kafkaproxy.config.KafkaProxyProperties;
import io.netty.channel.Channel;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Autowired;

@Component
public class ConnectionLogWriterFactory {
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter
            .ofPattern("uuuu-MM-dd'T'HH-mm-ss.SSS'Z'").withZone(ZoneOffset.UTC);
    private final KafkaProxyProperties properties;
    private final Clock clock;

    @Autowired
    public ConnectionLogWriterFactory(KafkaProxyProperties properties) {
        this(properties, Clock.systemUTC());
    }

    ConnectionLogWriterFactory(KafkaProxyProperties properties, Clock clock) {
        this.properties = properties;
        this.clock = clock;
    }

    public ConnectionLogWriter create(Channel clientChannel) {
        if (!properties.getRequestLogging().isEnabled()) {
            return new NoOpConnectionLogWriter();
        }
        String remote = safeRemote(clientChannel);
        String name = TIME_FORMAT.format(Instant.now(clock)) + "__client-" + remote + "__"
                + clientChannel.id().asShortText();
        return new FileConnectionLogWriter(properties.getRequestLogging().getBaseDirectory().resolve(name));
    }

    private String safeRemote(Channel channel) {
        if (channel.remoteAddress() instanceof InetSocketAddress address) {
            return sanitize(address.getHostString()) + "-" + address.getPort();
        }
        return "unknown";
    }

    private String sanitize(String value) {
        return value.replaceAll("[^A-Za-z0-9._-]", "_");
    }
}
