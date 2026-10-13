package io.github.panteliszara.warexis.browser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;

/** Owns only a runtime file successfully created by this publication. */
final class RuntimePublication implements AutoCloseable {
    @FunctionalInterface interface Writer { void write(Path path, String content) throws IOException; }
    private final Path path;
    private boolean owned;

    RuntimePublication(Path path) { this.path = path; }

    synchronized void publish(String content) throws IOException { publish(content, Files::writeString); }

    synchronized void publish(String content, Writer writer) throws IOException {
        Files.createFile(path, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
        owned = true;
        writer.write(path, content);
    }

    @Override public synchronized void close() throws IOException {
        if (owned) {
            Files.deleteIfExists(path);
            owned = false;
        }
    }
}
