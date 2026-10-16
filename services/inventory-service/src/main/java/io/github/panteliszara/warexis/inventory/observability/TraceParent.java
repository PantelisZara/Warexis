package io.github.panteliszara.warexis.inventory.observability;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;

/** Strict version-00 context codec; only the sampled flag is propagated. */
public final class TraceParent {
    private static final String HEADER = "traceparent";
    private static final int LENGTH = 55;

    private TraceParent() { }

    public static String normalize(String value) {
        if (value == null || value.length() != LENGTH
                || !value.startsWith("00-") || value.charAt(35) != '-' || value.charAt(52) != '-') {
            return null;
        }
        boolean traceNonzero = false;
        boolean spanNonzero = false;
        for (int i = 3; i < LENGTH; i++) {
            if (i == 35 || i == 52) continue;
            char digit = value.charAt(i);
            if (!isLowerHex(digit)) return null;
            if (i < 35 && digit != '0') traceNonzero = true;
            if (i > 35 && i < 52 && digit != '0') spanNonzero = true;
        }
        if (!traceNonzero || !spanNonzero) return null;
        int flags = Character.digit(value.charAt(54), 16);
        return value.substring(0, 53) + ((flags & 1) == 1 ? "01" : "00");
    }

    public static String fromHeaders(Headers headers) {
        if (headers == null) return null;
        try {
            byte[] value = null;
            boolean found = false;
            for (var header : headers) {
                if (!HEADER.equals(header.key())) continue;
                if (found) return null;
                found = true;
                value = header.value();
            }
            return value == null || value.length != LENGTH ? null
                    : normalize(new String(value, StandardCharsets.US_ASCII));
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    /** Replaces only the exact tracing header; telemetry failure must not prevent a send. */
    public static void inject(ProducerRecord<?, ?> record, String context) {
        if (record == null) return;
        String normalized = normalize(context);
        try {
            record.headers().remove(HEADER);
            if (normalized != null) {
                record.headers().add(HEADER, normalized.getBytes(StandardCharsets.US_ASCII));
            }
        } catch (RuntimeException ignored) {
            // Headers may already be read-only; business delivery remains independent.
        }
    }

    private static boolean isLowerHex(char digit) {
        return (digit >= '0' && digit <= '9') || (digit >= 'a' && digit <= 'f');
    }
}
