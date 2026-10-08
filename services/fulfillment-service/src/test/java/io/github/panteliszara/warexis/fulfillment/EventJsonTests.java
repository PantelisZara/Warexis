package io.github.panteliszara.warexis.fulfillment;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.json.JsonMapper;
import io.github.panteliszara.warexis.fulfillment.messaging.*;
import static org.assertj.core.api.Assertions.*;
class EventJsonTests {
    private final JsonMapper json=JsonMapper.builder().build();
    static String envelope(Confirmation in) { return """
        {"eventId":"%s","eventType":"%s","eventVersion":1,"occurredAt":"%s","aggregateId":"%s","correlationId":"%s","causationId":"%s",
        "payload":{"orderId":"%s","reservationId":"%s","lines":[{"orderLineId":"%s","sku":"A","quantity":1}]}}
        """.formatted(in.eventId(),in.eventType(),in.occurredAt(),in.orderId(),in.orderId(),in.causationId(),in.orderId(),in.reservationId(),in.lines().getFirst().orderLineId()); }
    static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    @Test void compatibleOptionalPropertiesAreIgnoredByKnownFingerprint() {
        var in=FulfillmentServiceTests.input();var raw=envelope(in);var root=(tools.jackson.databind.node.ObjectNode)json.readTree(raw);root.put("future",true);
        var payload=(tools.jackson.databind.node.ObjectNode)root.path("payload");payload.put("future",42);
        assertThat(EventJson.confirmation(bytes(in.orderId().toString()),bytes(root.toString()))).isEqualTo(EventJson.confirmation(bytes(in.orderId().toString()),bytes(raw)));
    }
    @ParameterizedTest @ValueSource(strings={"OrderPlaced","InventoryReserved","InventoryRejected","FulfillmentShipped","Unknown"})
    void wrongTriggerTypesAreNotConfirmation(String type) {
        var in=FulfillmentServiceTests.input();assertThatThrownBy(()->EventJson.confirmation(bytes(in.orderId().toString()),bytes(envelope(in).replace("OrderConfirmed",type))))
                .isInstanceOf(PoisonMessageException.class);
    }
    @ParameterizedTest @ValueSource(strings={"1.0","2","null","\"1\"","true"})
    void invalidVersionsArePoison(String version) {
        var in=FulfillmentServiceTests.input();assertThatThrownBy(()->EventJson.confirmation(bytes(in.orderId().toString()),bytes(envelope(in).replace("\"eventVersion\":1","\"eventVersion\":"+version))))
                .isInstanceOf(PoisonMessageException.class);
    }
    @Test void incompatibleInventoryReasonCannotBeConfirmation() {
        var in=FulfillmentServiceTests.input();var root=(tools.jackson.databind.node.ObjectNode)json.readTree(envelope(in));
        ((tools.jackson.databind.node.ObjectNode)root.path("payload")).put("reasonCode","UNKNOWN_SKU");
        assertThatThrownBy(()->EventJson.confirmation(bytes(in.orderId().toString()),bytes(root.toString()))).isInstanceOf(PoisonMessageException.class);
    }
    @Test void strictTransportRejectsDuplicatesTrailingUtf8AndWrongKey() {
        var in=FulfillmentServiceTests.input();var raw=envelope(in);
        for (var value:List.of(bytes(raw+" {}"),bytes(raw.replace("\"eventVersion\":1","\"eventVersion\":1,\"eventVersion\":1")),new byte[]{(byte)0xc3,0x28})) {
            assertThatThrownBy(()->EventJson.confirmation(bytes(in.orderId().toString()),value)).isInstanceOf(PoisonMessageException.class);
        }
        assertThatThrownBy(()->EventJson.confirmation(bytes(UUID.randomUUID().toString()),bytes(raw))).isInstanceOf(PoisonMessageException.class);
    }
    @Test void requiredCauseCanonicalUuidUtcAndUniqueLinesAreValidated() {
        var in=FulfillmentServiceTests.input();var root=(tools.jackson.databind.node.ObjectNode)json.readTree(envelope(in));
        var mutations=new ArrayList<String>();
        root.remove("causationId");mutations.add(root.toString());
        root=(tools.jackson.databind.node.ObjectNode)json.readTree(envelope(in));root.put("aggregateId",in.orderId().toString().toUpperCase());mutations.add(root.toString());
        root=(tools.jackson.databind.node.ObjectNode)json.readTree(envelope(in));root.put("occurredAt","2026-10-07T00:00:00+00:00");mutations.add(root.toString());
        root=(tools.jackson.databind.node.ObjectNode)json.readTree(envelope(in));var lines=(tools.jackson.databind.node.ArrayNode)root.path("payload").path("lines");lines.add(lines.get(0));mutations.add(root.toString());
        for(var raw:mutations) assertThatThrownBy(()->EventJson.confirmation(bytes(in.orderId().toString()),bytes(raw))).isInstanceOf(PoisonMessageException.class);
    }
}
