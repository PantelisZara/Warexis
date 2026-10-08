package io.github.panteliszara.warexis.order;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;
import io.github.panteliszara.warexis.order.messaging.EventJson;
import io.github.panteliszara.warexis.order.messaging.PoisonMessageException;

import static org.assertj.core.api.Assertions.*;

class OrderConfirmationContractTests {
    private final JsonMapper mapper=JsonMapper.builder().build();
    private final UUID orderId=UUID.randomUUID();

    @Test
    void validConfirmationAcceptsCompatibleOptionalFieldsAndOriginalRepeatedSkus() {
        var event=event();
        event.put("futureMetadata","compatible");
        ((ObjectNode)event.path("payload")).put("futurePayload",true);
        assertThatCode(()->EventJson.orderEvent(key(),bytes(event.toString()),"OrderConfirmed")).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @ValueSource(strings={"missingCause","nullCause","uppercaseCause","missingReservation","reasonCode","wrongAggregate",
            "wrongCorrelation","fractionalVersion","fractionalQuantity","duplicateLine","wrongKey","unsupportedType","unknownPublisherType"})
    void invalidRequiredContractOrIncompatibleOutcomeCannotBePublished(String vector) {
        var event=event();
        var payload=(ObjectNode)event.path("payload");
        var type="OrderConfirmed";
        var key=key();
        switch(vector) {
            case "missingCause" -> event.remove("causationId");
            case "nullCause" -> event.putNull("causationId");
            case "uppercaseCause" -> event.put("causationId","AABBCCDD-1111-2222-3333-444455556666");
            case "missingReservation" -> payload.remove("reservationId");
            case "reasonCode" -> payload.put("reasonCode","INSUFFICIENT_STOCK");
            case "wrongAggregate" -> event.put("aggregateId",UUID.randomUUID().toString());
            case "wrongCorrelation" -> event.put("correlationId",UUID.randomUUID().toString());
            case "fractionalVersion" -> event.put("eventVersion",1.0);
            case "fractionalQuantity" -> ((ObjectNode)payload.path("lines").get(0)).put("quantity",1.0);
            case "duplicateLine" -> ((ObjectNode)payload.path("lines").get(1)).put("orderLineId",payload.path("lines").get(0).path("orderLineId").asString());
            case "wrongKey" -> key=bytes(UUID.randomUUID().toString());
            case "unsupportedType" -> event.put("eventType","InventoryReserved");
            case "unknownPublisherType" -> type="InventoryReserved";
            default -> throw new AssertionError(vector);
        }
        var finalType=type;
        var finalKey=key;
        assertThatThrownBy(()->EventJson.orderEvent(finalKey,bytes(event.toString()),finalType)).isInstanceOf(PoisonMessageException.class);
    }

    @Test
    void duplicatePropertiesTrailingJsonAndMalformedUtf8CannotBePublished() {
        var json=event().toString();
        assertThatThrownBy(()->EventJson.orderConfirmed(key(),bytes(json+" {}"))).isInstanceOf(PoisonMessageException.class);
        assertThatThrownBy(()->EventJson.orderConfirmed(key(),bytes(json.replace("\"eventVersion\":1","\"eventVersion\":1,\"eventVersion\":1")))).isInstanceOf(PoisonMessageException.class);
        assertThatThrownBy(()->EventJson.orderConfirmed(key(),new byte[]{(byte)0xc3,0x28})).isInstanceOf(PoisonMessageException.class);
    }

    private ObjectNode event() {
        var event=mapper.createObjectNode();
        event.put("eventId",UUID.randomUUID().toString());event.put("eventType","OrderConfirmed");event.put("eventVersion",1);
        event.put("occurredAt","2026-10-07T00:00:00Z");event.put("aggregateId",orderId.toString());event.put("correlationId",orderId.toString());
        event.put("causationId",UUID.randomUUID().toString());
        var payload=event.putObject("payload");payload.put("orderId",orderId.toString());payload.put("reservationId",UUID.randomUUID().toString());
        var lines=payload.putArray("lines");
        for(int quantity: new int[]{2,3}) {
            var line=lines.addObject();line.put("orderLineId",UUID.randomUUID().toString());line.put("sku","Abc");line.put("quantity",quantity);
        }
        return event;
    }

    private byte[] key() { return bytes(orderId.toString()); }
    private byte[] bytes(String text) { return text.getBytes(StandardCharsets.UTF_8); }
}
