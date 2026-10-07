package io.github.panteliszara.warexis.inventory.messaging;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowEventValidatorTests {
    private final WorkflowEventValidator validator = new WorkflowEventValidator(JsonMapper.builder().build());
    static final String ORDER = "22222222-2222-4222-8222-222222222222";

    static String event() {
        return """
                {"eventId":"11111111-1111-4111-8111-111111111111","eventType":"OrderPlaced","eventVersion":1,
                "occurredAt":"2026-10-06T09:00:00Z","aggregateId":"22222222-2222-4222-8222-222222222222",
                "correlationId":"22222222-2222-4222-8222-222222222222","payload":{"orderId":"22222222-2222-4222-8222-222222222222",
                "lines":[{"orderLineId":"33333333-3333-4333-8333-333333333333","sku":"SKU-A","quantity":2}]}}
                """;
    }

    @Test
    void validatesRequiredContractAndToleratesCompatibleOptionalFields() {
        var valid = event().replace("\"eventVersion\":1", "\"futureOptional\":true,\"eventVersion\":1");
        assertThat(validator.validate(ORDER.getBytes(StandardCharsets.UTF_8), valid.getBytes(StandardCharsets.UTF_8)).order().orderId())
                .isEqualTo(UUID.fromString(ORDER));
    }

    @ParameterizedTest
    @MethodSource("invalidEvents")
    void rejectsMalformedIdentityTypesAndUnsupportedContracts(String invalid) {
        assertThatThrownBy(() -> validator.validate(ORDER.getBytes(StandardCharsets.UTF_8), invalid.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(InvalidEventException.class);
    }

    static Stream<String> invalidEvents() {
        return Stream.of("{", "null", "[]", event() + "{}", event().replace("\"eventVersion\":1", "\"eventVersion\":1,\"eventVersion\":1"),
                event().replace("\"eventVersion\":1", "\"eventVersion\":2"), event().replace("\"eventVersion\":1", "\"eventVersion\":\"1\""),
                event().replace("\"eventVersion\":1", "\"eventVersion\":1.0"), event().replace("OrderPlaced", "Unknown"),
                event().replace("\"quantity\":2", "\"quantity\":\"2\""), event().replace("\"quantity\":2", "\"quantity\":2.0"),
                event().replace("\"quantity\":2", "\"quantity\":2147483648"), event().replace("\"quantity\":2", "\"quantity\":0"),
                event().replace("SKU-A", " SKU-A"), event().replace("09:00:00Z", "09:00:00+00:00"),
                event().replace("11111111-1111-4111-8111-111111111111", "1-1-1-1-1"),
                event().replace("\"aggregateId\":\"" + ORDER + "\"", "\"aggregateId\":\"11111111-1111-4111-8111-111111111111\""),
                event().replace("\"correlationId\":\"" + ORDER + "\"", "\"correlationId\":null"));
    }

    @Test
    void rejectsWrongKafkaKeyNullAndMalformedUtf8WithoutReplacement() {
        assertThatThrownBy(() -> validator.validate("wrong".getBytes(StandardCharsets.UTF_8), event().getBytes(StandardCharsets.UTF_8))).isInstanceOf(InvalidEventException.class);
        assertThatThrownBy(() -> validator.validate(null, null)).isInstanceOf(InvalidEventException.class);
        assertThatThrownBy(() -> validator.validate(ORDER.getBytes(StandardCharsets.UTF_8), new byte[]{(byte) 0xc3, 0x28})).isInstanceOf(InvalidEventException.class);
    }
}
