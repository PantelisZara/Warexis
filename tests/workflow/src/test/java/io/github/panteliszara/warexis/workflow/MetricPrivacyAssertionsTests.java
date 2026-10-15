package io.github.panteliszara.warexis.workflow;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MetricPrivacyAssertionsTests {
    private static final UUID ORDER_ID = UUID.fromString("00000000-0000-0000-0000-000000000001");

    @Test
    void permitsBoundedFrameworkAuthorizationFilterName() {
        MetricPrivacyAssertions.assertSafe("spring_security_filterchains_seconds_count{"
                + "service=\"order\",spring_security_reached_filter_name=\"AuthorizationFilter\"} 1", ORDER_ID);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "metric{Authorization=\"redacted\"} 1",
            "metric{authorization=\"redacted\"} 1",
            "Authorization: redacted",
            "metric{header=\"Bearer redacted\"} 1",
            "metric{token=\"eyJhbGciOiJub25lIn0.eyJzdWIiOiJmaXh0dXJlIn0.signature\"} 1",
            "metric{event_id=\"fixture\"} 1",
            "metric{order_id=\"fixture\"} 1",
            "metric{sku=\"fixture\"} 1",
            "metric{value=\"00000000-0000-0000-0000-000000000001\"} 1"
    })
    void rejectsSensitiveLabelsHeadersTokensAndKnownBusinessIdentity(String body) {
        assertThatThrownBy(() -> MetricPrivacyAssertions.assertSafe(body, ORDER_ID))
                .isInstanceOf(AssertionError.class);
    }
}
