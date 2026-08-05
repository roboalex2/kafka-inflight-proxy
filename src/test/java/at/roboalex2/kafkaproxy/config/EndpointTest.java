package at.roboalex2.kafkaproxy.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import org.junit.jupiter.api.Test;

class EndpointTest {
    @Test
    void parsesHostnamesIpv4AndBracketedIpv6() {
        assertThat(Endpoint.parse("kafka-1:9092")).isEqualTo(new Endpoint("kafka-1", 9092));
        assertThat(Endpoint.parse("127.0.0.1:19092")).isEqualTo(new Endpoint("127.0.0.1", 19092));
        assertThat(Endpoint.parse("[2001:db8::1]:9092"))
                .isEqualTo(new Endpoint("2001:db8::1", 9092)).hasToString("[2001:db8::1]:9092");
    }

    @Test
    void rejectsMalformedEndpointsWithUsefulMessages() {
        assertThatIllegalArgumentException().isThrownBy(() -> Endpoint.parse("2001:db8::1:9092"))
                .withMessageContaining("IPv6 addresses must be bracketed");
        assertThatIllegalArgumentException().isThrownBy(() -> Endpoint.parse("kafka-1:70000"))
                .withMessageContaining("port must be between 1 and 65535");
        assertThatIllegalArgumentException().isThrownBy(() -> Endpoint.parse("kafka-1"))
                .withMessageContaining("expected host:port");
    }
}
