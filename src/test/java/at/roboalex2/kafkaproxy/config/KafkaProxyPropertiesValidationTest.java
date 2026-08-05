package at.roboalex2.kafkaproxy.config;

import static org.assertj.core.api.Assertions.assertThat;
import at.roboalex2.kafkaproxy.KafkaProxyApplication;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.env.MapPropertySource;

class KafkaProxyPropertiesValidationTest {
    private static final String BROKER_MAPPING_PROPERTY =
            "kafka-proxy.broker-proxy-addresses.[kafka-1:9092]";

    @Test
    void bindsAllTypedConfiguration() {
        contextRunner(Map.of()).run(context -> {
            assertThat(context).hasNotFailed();
            KafkaProxyProperties properties = context.getBean(KafkaProxyProperties.class);
            assertThat(properties.getListenAddress()).isEqualTo(new Endpoint("0.0.0.0", 19092));
            assertThat(properties.getUpstreamBrokerAddress()).isEqualTo(new Endpoint("kafka-1", 9092));
            assertThat(properties.getBrokerProxyAddresses()).containsEntry(
                    new Endpoint("kafka-1", 9092), new Endpoint("proxy-1", 19092));
            assertThat(properties.getProtocol().getMaxFrameSizeBytes()).isEqualTo(104_857_600);
            assertThat(properties.getRequestLogging().isEnabled()).isTrue();
            assertThat(properties.getRequestLogging().getBaseDirectory())
                    .isEqualTo(Path.of("build/protocol-logs"));
        });
    }

    @Test
    void rejectsMalformedListenEndpointAtStartup() {
        contextRunner(Map.of("kafka-proxy.listen-address", "missing-port")).run(context ->
                assertThat(context).hasFailed().getFailure().rootCause()
                        .hasMessageContaining("Invalid endpoint 'missing-port'")
                        .hasMessageContaining("expected host:port"));
    }

    @Test
    void rejectsMalformedBrokerMapValueAtStartup() {
        contextRunner(Map.of(BROKER_MAPPING_PROPERTY, "proxy-1:70000")).run(context ->
                assertThat(context).hasFailed().getFailure().rootCause()
                        .hasMessageContaining("Invalid endpoint 'proxy-1:70000'")
                        .hasMessageContaining("port must be between 1 and 65535"));
    }

    @Test
    void rejectsMalformedBrokerMapKeyAtStartup() {
        contextRunner(Map.of(
                "kafka-proxy.broker-proxy-addresses.[missing-port]", "proxy-2:19092"))
                .run(context -> assertThat(context).hasFailed().getFailure().rootCause()
                        .hasMessageContaining("Invalid endpoint 'missing-port'")
                        .hasMessageContaining("expected host:port"));
    }

    @Test
    void rejectsEmptyBrokerMapAtStartup() {
        Map<String, Object> properties = validProperties();
        properties.remove(BROKER_MAPPING_PROPERTY);
        contextRunnerFor(properties)
                .run(context -> assertThat(context).hasFailed().getFailure()
                        .rootCause()
                        .hasMessageContaining("brokerProxyAddresses").hasMessageContaining("at least one"));
    }

    @Test
    void rejectsNonPositiveMaximumFrameSizeAtStartup() {
        contextRunner(Map.of("kafka-proxy.protocol.max-frame-size-bytes", "0")).run(context ->
                assertThat(context).hasFailed().getFailure()
                        .rootCause()
                        .hasMessageContaining("maxFrameSizeBytes").hasMessageContaining("at least 1 byte"));
    }

    private ApplicationContextRunner contextRunner(Map<String, Object> overrides) {
        Map<String, Object> properties = validProperties();
        properties.putAll(overrides);
        return contextRunnerFor(properties);
    }

    private ApplicationContextRunner contextRunnerFor(Map<String, Object> properties) {
        Map<String, Object> source = new LinkedHashMap<>(properties);
        return new ApplicationContextRunner()
                .withUserConfiguration(KafkaProxyApplication.class)
                .withInitializer(context -> context.getEnvironment().getPropertySources()
                        .addFirst(new MapPropertySource("kafka-proxy-validation", source)));
    }

    private Map<String, Object> validProperties() {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("spring.main.web-application-type", "none");
        properties.put("spring.config.name", "kafka-proxy-validation-test");
        properties.put("kafka-proxy.listen-address", "0.0.0.0:19092");
        properties.put("kafka-proxy.upstream-broker-address", "kafka-1:9092");
        properties.put(BROKER_MAPPING_PROPERTY, "proxy-1:19092");
        properties.put("kafka-proxy.protocol.max-frame-size-bytes", "104857600");
        properties.put("kafka-proxy.request-logging.enabled", "true");
        properties.put("kafka-proxy.request-logging.base-directory", "./build/protocol-logs");
        return properties;
    }
}
