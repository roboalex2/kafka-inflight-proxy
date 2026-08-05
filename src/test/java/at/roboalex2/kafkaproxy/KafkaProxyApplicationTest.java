package at.roboalex2.kafkaproxy;

import static org.assertj.core.api.Assertions.assertThat;
import at.roboalex2.kafkaproxy.config.KafkaProxyProperties;
import at.roboalex2.kafkaproxy.network.KafkaProxyServer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {"kafka-proxy.server.enabled=false", "kafka-proxy.crypto.key-encryption-key=test-only-kek"})
class KafkaProxyApplicationTest {
    @Autowired private ApplicationContext applicationContext;
    @Autowired private KafkaProxyProperties properties;

    @Test
    void startsContextWithSampleConfigurationWithoutStartingNetworkServer() {
        assertThat(properties.getListenAddress().getPort()).isEqualTo(19092);
        assertThat(applicationContext.getBeansOfType(KafkaProxyServer.class)).isEmpty();
    }
}
