package at.roboalex2.kafkaproxy.config;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import org.hibernate.validator.constraints.time.DurationMin;

public class RedisProperties {
    @NotBlank(message = "must not be blank")
    private String host = "redis";
    @Min(value = 1, message = "must be between 1 and 65535")
    @Max(value = 65_535, message = "must be between 1 and 65535")
    private int port = 6379;
    @Min(value = 0, message = "must be non-negative")
    private int database;
    private String username;
    private String password;
    @NotNull @DurationMin(millis = 1, message = "must be positive")
    private Duration connectTimeout = Duration.ofSeconds(2);
    @NotNull @DurationMin(millis = 1, message = "must be positive")
    private Duration commandTimeout = Duration.ofSeconds(2);

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }
    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }
    public int getDatabase() { return database; }
    public void setDatabase(int database) { this.database = database; }
    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }
    public String getPassword() { return password; }
    public void setPassword(String password) { this.password = password; }
    public Duration getConnectTimeout() { return connectTimeout; }
    public void setConnectTimeout(Duration connectTimeout) { this.connectTimeout = connectTimeout; }
    public Duration getCommandTimeout() { return commandTimeout; }
    public void setCommandTimeout(Duration commandTimeout) { this.commandTimeout = commandTimeout; }
}
