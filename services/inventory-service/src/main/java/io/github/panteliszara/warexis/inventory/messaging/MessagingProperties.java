package io.github.panteliszara.warexis.inventory.messaging;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("warexis.messaging")
public record MessagingProperties(boolean enabled, Duration retryBaseDelay, Duration pollInterval) {
    public MessagingProperties {
        if (retryBaseDelay == null || retryBaseDelay.isNegative() || retryBaseDelay.isZero()
                || retryBaseDelay.compareTo(Duration.ofSeconds(1)) > 0) {
            throw new IllegalArgumentException("Retry base delay must be greater than zero and at most one second.");
        }
        if (pollInterval == null || pollInterval.isNegative() || pollInterval.isZero()) {
            throw new IllegalArgumentException("Polling interval must be positive.");
        }
    }
    public Duration delay(int attempt) { return retryBaseDelay.multipliedBy(1L << (attempt - 1)); }
}
