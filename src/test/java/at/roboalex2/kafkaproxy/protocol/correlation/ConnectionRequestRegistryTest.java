package at.roboalex2.kafkaproxy.protocol.correlation;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class ConnectionRequestRegistryTest {
    @Test
    void isolatesConnectionsRejectsDuplicatesAndClearsPendingRequests() {
        ConnectionRequestRegistry first = new ConnectionRequestRegistry();
        ConnectionRequestRegistry second = new ConnectionRequestRegistry();
        RequestContext firstRequest = request("first", 42, true);
        RequestContext secondRequest = request("second", 42, true);

        assertThat(first.register(firstRequest)).isTrue();
        assertThat(second.register(secondRequest)).isTrue();
        assertThat(first.register(request("first", 42, true))).isFalse();
        assertThat(first.remove(42)).isSameAs(firstRequest);
        assertThat(second.remove(42)).isSameAs(secondRequest);

        first.register(request("first", 7, true));
        first.clear();
        assertThat(first.size()).isZero();
    }

    @Test
    void responseFreeRequestIsNeverRetained() {
        ConnectionRequestRegistry registry = new ConnectionRequestRegistry();
        assertThat(registry.register(request("connection", 9, false))).isTrue();
        assertThat(registry.size()).isZero();
    }

    private RequestContext request(String connection, int correlationId, boolean expectsResponse) {
        return new RequestContext(connection, correlationId, (short) 0, "Produce", (short) 13,
                (short) 2, (short) 1, expectsResponse, 1, Instant.EPOCH);
    }
}
