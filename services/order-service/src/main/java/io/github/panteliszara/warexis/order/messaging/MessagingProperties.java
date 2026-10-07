package io.github.panteliszara.warexis.order.messaging;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("warexis.messaging")
public record MessagingProperties(boolean enabled, Duration retryBaseDelay, Duration pollInterval) {
    public MessagingProperties {
        if (retryBaseDelay == null || retryBaseDelay.isNegative() || retryBaseDelay.isZero() || retryBaseDelay.compareTo(Duration.ofSeconds(1)) > 0
                || pollInterval == null || pollInterval.isNegative() || pollInterval.isZero()) {
            throw new IllegalArgumentException("Retry base must be positive and at most one second; polling must be positive.");
        }
    }
}
