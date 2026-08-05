package at.roboalex2.kafkaproxy.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "kafka-proxy")
public class KafkaProxyProperties {
    @NotNull(message = "must be configured as host:port")
    private Endpoint listenAddress;
    @NotNull(message = "must be configured as host:port")
    private Endpoint upstreamBrokerAddress;
    @NotEmpty(message = "must contain at least one broker-to-proxy endpoint mapping")
    private Map<@NotNull Endpoint, @NotNull Endpoint> brokerProxyAddresses = new LinkedHashMap<>();
    @Valid @NotNull
    private ProtocolProperties protocol = new ProtocolProperties();
    @Valid @NotNull
    private RequestLoggingProperties requestLogging = new RequestLoggingProperties();
    @Valid @NotNull
    private ServerProperties server = new ServerProperties();

    public Endpoint getListenAddress() {
        return listenAddress;
    }

    public void setListenAddress(Endpoint listenAddress) {
        this.listenAddress = listenAddress;
    }

    public Endpoint getUpstreamBrokerAddress() {
        return upstreamBrokerAddress;
    }

    public void setUpstreamBrokerAddress(Endpoint upstreamBrokerAddress) {
        this.upstreamBrokerAddress = upstreamBrokerAddress;
    }

    public Map<Endpoint, Endpoint> getBrokerProxyAddresses() {
        return brokerProxyAddresses;
    }

    public void setBrokerProxyAddresses(Map<Endpoint, Endpoint> brokerProxyAddresses) {
        this.brokerProxyAddresses = brokerProxyAddresses;
    }

    public ProtocolProperties getProtocol() {
        return protocol;
    }

    public void setProtocol(ProtocolProperties protocol) {
        this.protocol = protocol;
    }

    public RequestLoggingProperties getRequestLogging() {
        return requestLogging;
    }

    public void setRequestLogging(RequestLoggingProperties requestLogging) {
        this.requestLogging = requestLogging;
    }

    public ServerProperties getServer() {
        return server;
    }

    public void setServer(ServerProperties server) {
        this.server = server;
    }
}
