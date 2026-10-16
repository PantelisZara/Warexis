package io.github.panteliszara.warexis.fulfillment.observability;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class TraceParentTests {
    private static final String PREFIX = "00-1234567890abcdef1234567890abcdef-1234567890abcdef-";
    private static final String SAMPLED = PREFIX + "01";
    private static final String UNSAMPLED = PREFIX + "00";

    @ParameterizedTest
    @MethodSource("flags")
    void everyValidFlagByteRetainsOnlySampledBit(int flags) {
        String input = PREFIX + String.format(Locale.ROOT, "%02x", flags);
        String expected = PREFIX + ((flags & 1) == 1 ? "01" : "00");
        assertThat(TraceParent.normalize(input)).isEqualTo(expected);
        assertThat(TraceParent.normalize(expected)).isEqualTo(expected);
        assertThat(TraceParent.fromHeaders(new RecordHeaders().add("traceparent", bytes(input))))
                .isEqualTo(expected);
        var record = new ProducerRecord<>("events", "key", "body");
        TraceParent.inject(record, input);
        assertThat(record.headers().lastHeader("traceparent").value()).isEqualTo(bytes(expected));
    }

    static IntStream flags() { return IntStream.range(0, 256); }

    @ParameterizedTest
    @MethodSource("invalidContexts")
    void malformedContextIsRejectedBeforePropagation(String context) {
        assertThat(TraceParent.normalize(context)).isNull();
        assertThat(TraceParent.fromHeaders(new RecordHeaders().add("traceparent",
                context == null ? null : bytes(context)))).isNull();
        var record = new ProducerRecord<>("events", "key", "body");
        record.headers().add("traceparent", bytes(SAMPLED));
        TraceParent.inject(record, context);
        assertThat(record.headers().headers("traceparent")).isEmpty();
    }

    static Stream<String> invalidContexts() {
        return Stream.of(null, "", " ", SAMPLED.substring(1), SAMPLED + "0", SAMPLED + "-extra",
                " " + SAMPLED, SAMPLED + "\n", "x".repeat(10_000), SAMPLED.toUpperCase(Locale.ROOT),
                "01" + SAMPLED.substring(2), "ff" + SAMPLED.substring(2),
                "00-" + "0".repeat(32) + "-1234567890abcdef-01",
                "00-1234567890abcdef1234567890abcdef-" + "0".repeat(16) + "-01",
                PREFIX + "0A", PREFIX + "0g", PREFIX + "-1", PREFIX + "０1",
                SAMPLED.substring(0, 10) + "g" + SAMPLED.substring(11),
                SAMPLED.substring(0, 40) + "G" + SAMPLED.substring(41),
                SAMPLED.substring(0, 35) + ":" + SAMPLED.substring(36),
                SAMPLED.substring(0, 52) + ":" + SAMPLED.substring(53));
    }

    @Test void missingAndDuplicateExactHeadersNeverSupplyContext() {
        assertThat(TraceParent.fromHeaders(null)).isNull();
        assertThat(TraceParent.fromHeaders(new RecordHeaders())).isNull();
        var headers = new RecordHeaders().add("Traceparent", bytes(SAMPLED))
                .add("baggage", bytes("private=value")).add("tracestate", bytes("vendor=value"));
        assertThat(TraceParent.fromHeaders(headers)).isNull();
        headers.add("traceparent", bytes(UNSAMPLED));
        assertThat(TraceParent.fromHeaders(headers)).isEqualTo(UNSAMPLED);
        headers.add("traceparent", bytes(UNSAMPLED));
        assertThat(TraceParent.fromHeaders(headers)).isNull();
        assertThat(TraceParent.fromHeaders(new RecordHeaders().add("traceparent", null)
                .add("traceparent", bytes(SAMPLED)))).isNull();
        assertThat(TraceParent.fromHeaders(new RecordHeaders().add("traceparent", bytes(SAMPLED))
                .add("traceparent", bytes("invalid")))).isNull();
    }

    @Test void nonAsciiTransportBytesAreRejectedWithoutChangingHeaders() {
        byte[] value = bytes(SAMPLED);
        value[4] = (byte) 0xff;
        var header = new RecordHeader("traceparent", value);
        var headers = new RecordHeaders(new Header[] { header });
        assertThat(TraceParent.fromHeaders(headers)).isNull();
        assertThat(headers.toArray()).containsExactly(header);
        assertThat(header.value()).isSameAs(value);
    }

    @Test void injectionPreservesAllDiagnosticHeadersAndImmutableBusinessRecord() {
        byte[] key = bytes("business-key");
        byte[] payload = bytes("{\"eventType\":\"OrderPlaced\",\"orderId\":\"unchanged\"}");
        var record = new ProducerRecord<byte[], byte[]>("dead-letter", 2, 1234L, key, payload);
        String[] names = {"warexis-source-topic", "warexis-source-partition", "warexis-source-offset",
                "warexis-consumer-group", "warexis-failure-code", "warexis-processing-attempts",
                "warexis-dlt-attempts", "warexis-failed-at"};
        for (String name : names) record.headers().add(name, bytes("diagnostic:" + name));
        record.headers().add("Traceparent", bytes("case-sensitive-unrelated-header"));
        Header[] diagnostics = record.headers().toArray();
        record.headers().add("traceparent", bytes(SAMPLED)).add("traceparent", bytes("invalid"));
        TraceParent.inject(record, PREFIX + "fe");
        assertThat(record.headers().toArray()).hasSize(diagnostics.length + 1);
        assertThat(record.headers().headers("traceparent")).hasSize(1);
        assertThat(TraceParent.fromHeaders(record.headers())).isEqualTo(UNSAMPLED);
        TraceParent.inject(record, PREFIX + "ff");
        assertThat(record.headers().headers("traceparent")).hasSize(1);
        assertThat(TraceParent.fromHeaders(record.headers())).isEqualTo(SAMPLED);
        TraceParent.inject(record, null);
        assertThat(record.headers().toArray()).containsExactly(diagnostics);
        for (int i = 0; i < diagnostics.length; i++) {
            assertThat(record.headers().toArray()[i]).isSameAs(diagnostics[i]);
            assertThat(record.headers().toArray()[i].value()).isSameAs(diagnostics[i].value());
        }
        assertThat(record.topic()).isEqualTo("dead-letter");
        assertThat(record.partition()).isEqualTo(2);
        assertThat(record.timestamp()).isEqualTo(1234L);
        assertThat(record.key()).isSameAs(key).isEqualTo(bytes("business-key"));
        assertThat(record.value()).isSameAs(payload)
                .isEqualTo(bytes("{\"eventType\":\"OrderPlaced\",\"orderId\":\"unchanged\"}"));
    }

    @Test void transportFailuresCannotBreakBusinessDelivery() {
        var headers = mock(Headers.class);
        when(headers.iterator()).thenThrow(new IllegalStateException("unavailable"));
        assertThat(TraceParent.fromHeaders(headers)).isNull();
        var record = new ProducerRecord<>("events", "key", "body");
        record.headers().add("traceparent", bytes(UNSAMPLED));
        ((RecordHeaders) record.headers()).setReadOnly();
        assertThatCode(() -> TraceParent.inject(record, SAMPLED)).doesNotThrowAnyException();
        assertThat(TraceParent.fromHeaders(record.headers())).isEqualTo(UNSAMPLED);
        assertThatCode(() -> TraceParent.inject(null, SAMPLED)).doesNotThrowAnyException();
    }

    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
}
