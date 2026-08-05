package at.roboalex2.kafkaproxy.config;

import java.util.Objects;

/** A validated TCP endpoint supporting hostnames, IPv4, and bracketed IPv6. */
public final class Endpoint {
    private final String host;
    private final int port;

    public Endpoint(String host, int port) {
        this.host = validateHost(host);
        this.port = validatePort(port);
    }

    public static Endpoint parse(String value) {
        if (value == null || value.isBlank()) {
            throw invalid(value, "the value is blank");
        }
        String endpoint = value.trim();
        String host;
        String portText;
        if (endpoint.startsWith("[")) {
            int closingBracket = endpoint.indexOf(']');
            if (closingBracket < 0 || closingBracket + 1 >= endpoint.length()
                    || endpoint.charAt(closingBracket + 1) != ':') {
                throw invalid(value, "bracketed IPv6 must use [host]:port");
            }
            host = endpoint.substring(1, closingBracket);
            portText = endpoint.substring(closingBracket + 2);
        } else {
            int separator = endpoint.lastIndexOf(':');
            if (separator <= 0 || separator != endpoint.indexOf(':')) {
                throw invalid(value, "expected host:port; IPv6 addresses must be bracketed");
            }
            host = endpoint.substring(0, separator);
            portText = endpoint.substring(separator + 1);
        }
        if (portText.isBlank() || !portText.chars().allMatch(Character::isDigit)) {
            throw invalid(value, "port must be a decimal number between 1 and 65535");
        }
        try {
            return new Endpoint(host, Integer.parseInt(portText));
        } catch (NumberFormatException exception) {
            throw invalid(value, "port must be a decimal number between 1 and 65535");
        } catch (IllegalArgumentException exception) {
            throw invalid(value, exception.getMessage());
        }
    }

    public String getHost() { return host; }
    public int getPort() { return port; }

    @Override
    public String toString() {
        String renderedHost = host.indexOf(':') >= 0 ? "[" + host + "]" : host;
        return renderedHost + ":" + port;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof Endpoint endpoint)) return false;
        return port == endpoint.port && host.equals(endpoint.host);
    }

    @Override
    public int hashCode() { return Objects.hash(host, port); }

    private static String validateHost(String host) {
        if (host == null || host.isBlank()) throw new IllegalArgumentException("host must not be blank");
        if (!host.equals(host.trim()) || host.chars().anyMatch(Character::isWhitespace)
                || host.indexOf('[') >= 0 || host.indexOf(']') >= 0) {
            throw new IllegalArgumentException("host contains invalid whitespace or brackets");
        }
        return host;
    }

    private static int validatePort(int port) {
        if (port < 1 || port > 65_535) throw new IllegalArgumentException("port must be between 1 and 65535");
        return port;
    }

    private static IllegalArgumentException invalid(String value, String reason) {
        return new IllegalArgumentException("Invalid endpoint '" + value + "': " + reason);
    }
}
