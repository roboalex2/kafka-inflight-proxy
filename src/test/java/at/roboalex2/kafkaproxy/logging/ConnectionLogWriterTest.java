package at.roboalex2.kafkaproxy.logging;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConnectionLogWriterTest {
    @TempDir Path temporaryDirectory;

    @Test
    void createsOneConnectionDirectoryAndOneOrderedLogFile() throws Exception {
        Path connection = temporaryDirectory.resolve("connection-1");
        FileConnectionLogWriter writer = new FileConnectionLogWriter(connection);
        writer.append("1 C -> B: first");
        writer.append("2 B -> C: second");
        writer.close();

        assertThat(Files.list(temporaryDirectory).toList()).containsExactly(connection);
        assertThat(Files.list(connection).toList()).containsExactly(connection.resolve("connection.log"));
        assertThat(Files.readAllLines(connection.resolve("connection.log")))
                .containsExactly("1 C -> B: first", "2 B -> C: second");
    }

    @Test
    void disabledWriterHasNoFilesystemSideEffects() throws Exception {
        NoOpConnectionLogWriter writer = new NoOpConnectionLogWriter();
        writer.append("ignored");
        writer.close();
        assertThat(Files.list(temporaryDirectory)).isEmpty();
    }
}
