package at.roboalex2.kafkaproxy.config;

import jakarta.validation.constraints.Min;

public class ServerProperties {
    private boolean enabled = true;

    @Min(value = 1, message = "must be at least 1 millisecond")
    private int connectTimeoutMillis = 10_000;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    public void setConnectTimeoutMillis(int connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
    }
}
