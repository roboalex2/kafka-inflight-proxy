package at.roboalex2.kafkaproxy.config;

import jakarta.validation.constraints.NotNull;
import java.nio.file.Path;

public class RequestLoggingProperties {
    private boolean enabled;
    @NotNull(message = "must be configured")
    private Path baseDirectory = Path.of("./logs/kafka-proxy");

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Path getBaseDirectory() {
        return baseDirectory;
    }

    public void setBaseDirectory(Path baseDirectory) {
        this.baseDirectory = baseDirectory;
    }
}
