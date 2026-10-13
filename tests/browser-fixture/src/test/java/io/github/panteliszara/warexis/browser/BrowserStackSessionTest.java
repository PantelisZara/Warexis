package io.github.panteliszara.warexis.browser;

import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@EnabledIfSystemProperty(named = "fixture.start", matches = "true")
class BrowserStackSessionTest {
    @Test
    void keepDisposableStackAvailableUntilStopped() throws Exception {
        var config = FixtureConfig.fromProperties();
        try (var stack = new BrowserStack(config)) {
            if (Files.exists(stack.stopFile())) throw new IllegalArgumentException("Stop file already exists; use a new session path");
            var shutdown = new Thread(stack::close, "browser-fixture-cleanup");
            Runtime.getRuntime().addShutdownHook(shutdown);
            try {
                stack.start();
                var deadline = Instant.now().plus(Duration.ofHours(8));
                while (!Files.exists(stack.stopFile()) && Instant.now().isBefore(deadline)) Thread.sleep(250);
                if (!Files.exists(stack.stopFile())) throw new IllegalStateException("Fixture session reached its eight-hour safety deadline");
            } finally {
                Runtime.getRuntime().removeShutdownHook(shutdown);
            }
        }
    }
}
