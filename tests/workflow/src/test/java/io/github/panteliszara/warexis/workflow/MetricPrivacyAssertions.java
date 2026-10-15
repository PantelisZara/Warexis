package io.github.panteliszara.warexis.workflow;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Checks exposed values without rejecting Spring Security's bounded AuthorizationFilter label. */
final class MetricPrivacyAssertions {
    private MetricPrivacyAssertions() { }

    static void assertSafe(String body, UUID orderId) {
        assertThat(body).doesNotContain(orderId.toString(), "event_id=", "order_id=", "sku=", "Bearer ")
                .doesNotContainPattern("(?i)\\bauthorization\\s*[:=]")
                .doesNotContainPattern("\\beyJ[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+\\b");
    }
}
