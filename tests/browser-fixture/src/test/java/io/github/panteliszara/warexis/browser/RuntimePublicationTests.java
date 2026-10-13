package io.github.panteliszara.warexis.browser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RuntimePublicationTests {
    @TempDir Path directory;

    @Test void failedWriteRemovesOwnedPartialCredentialFile() throws Exception {
        var file = directory.resolve("runtime.json");
        try (var publication = new RuntimePublication(file)) {
            assertThatThrownBy(() -> publication.publish("sample", (path, content) -> {
                Files.writeString(path, "partial");
                throw new IOException("simulated disk write failure");
            })).isInstanceOf(IOException.class);
            assertThat(Files.getPosixFilePermissions(file)).isEqualTo(PosixFilePermissions.fromString("rw-------"));
        }
        assertThat(file).doesNotExist();
    }

    @Test void failedCreationNeverDeletesPreExistingUserFile() throws Exception {
        var file = directory.resolve("runtime.json");
        Files.writeString(file, "pre-existing user content");
        try (var publication = new RuntimePublication(file)) {
            assertThatThrownBy(() -> publication.publish("sample")).isInstanceOf(IOException.class);
        }
        assertThat(Files.readString(file)).isEqualTo("pre-existing user content");
    }

    @Test void successfulPublicationRemovesCredentialsOnClose() throws Exception {
        var file = directory.resolve("runtime.json");
        var publication = new RuntimePublication(file);
        publication.publish("sample");
        assertThat(Files.readString(file)).isEqualTo("sample");
        publication.close();
        publication.close();
        assertThat(file).doesNotExist();
    }
}
