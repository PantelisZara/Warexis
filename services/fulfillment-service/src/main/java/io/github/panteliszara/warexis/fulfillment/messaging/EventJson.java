package io.github.panteliszara.warexis.fulfillment.messaging;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
public final class EventJson {
    private static final JsonMapper JSON = JsonMapper.builder()
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).build();
    private EventJson() {}
    public static Confirmation confirmation(byte[] key, byte[] value) {
        try {
            var root = envelope(key, value);
            var type = text(root, "eventType");
            var order = uuid(root.path("payload"), "orderId");
            if (!uuid(root, "aggregateId").equals(order)) throw new PoisonMessageException();
            UUID reservation = null, cause = null;
            if (type.equals("OrderConfirmed")) {
                if (root.path("payload").has("reasonCode")) throw new PoisonMessageException();
                reservation = uuid(root.path("payload"), "reservationId");
                cause = uuid(root, "causationId");
            } else if (!type.equals("OrderPlaced") || root.has("causationId")) throw new PoisonMessageException();
            return new Confirmation(uuid(root,"eventId"), type, timestamp(root), order, reservation, cause,
                    lines(root.path("payload")));
        } catch (RuntimeException ex) { throw new PoisonMessageException(); }
    }
    public static void shipped(byte[] key, byte[] value) {
        try {
            var root = envelope(key, value);
            var payload = root.path("payload");
            if (!text(root,"eventType").equals("FulfillmentShipped") || root.has("causationId")
                    || !uuid(root,"aggregateId").equals(uuid(payload,"fulfillmentId"))
                    || !text(payload,"shipmentReference").matches("[A-Za-z0-9][A-Za-z0-9._-]{0,63}")) throw new PoisonMessageException();
            uuid(payload,"reservationId"); uuid(payload,"orderConfirmedEventId");
            var shipped = text(payload,"shippedAt");
            if (!shipped.endsWith("Z") || !Instant.parse(shipped).equals(timestamp(root))) throw new PoisonMessageException();
            lines(payload);
        } catch (RuntimeException ex) { throw new PoisonMessageException(); }
    }
    private static JsonNode envelope(byte[] key, byte[] value) {
        var event = JSON.readTree(utf8(value));
        if (event == null || !event.isObject() || !event.path("payload").isObject()
                || integer(event, "eventVersion") != 1) {
            throw new PoisonMessageException();
        }
        uuid(event, "eventId");
        uuid(event, "aggregateId");
        text(event, "eventType");
        timestamp(event);
        var orderId = uuid(event.path("payload"), "orderId");
        if (!utf8(key).equals(orderId.toString()) || !uuid(event, "correlationId").equals(orderId)) {
            throw new PoisonMessageException();
        }
        return event;
    }

    private static List<Confirmation.Line> lines(JsonNode payload) {
        var input = payload.path("lines");
        if (!input.isArray() || input.isEmpty() || input.size() > 100) {
            throw new PoisonMessageException();
        }
        var ids = new HashSet<UUID>();
        var lines = new ArrayList<Confirmation.Line>();
        for (var line : input) {
            var id = uuid(line, "orderLineId");
            var sku = text(line, "sku");
            var quantity = integer(line, "quantity");
            if (!ids.add(id) || !sku.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,63}") || quantity <= 0) {
                throw new PoisonMessageException();
            }
            lines.add(new Confirmation.Line(id, sku, quantity));
        }
        return List.copyOf(lines);
    }

    private static UUID uuid(JsonNode node, String field) {
        var value = text(node, field);
        var id = UUID.fromString(value);
        if (!id.toString().equals(value)) {
            throw new PoisonMessageException();
        }
        return id;
    }

    private static Instant timestamp(JsonNode node) {
        var value = text(node, "occurredAt");
        if (!value.endsWith("Z")) {
            throw new PoisonMessageException();
        }
        return Instant.parse(value);
    }

    private static String text(JsonNode node, String field) {
        var value = node.path(field);
        if (!value.isString()) {
            throw new PoisonMessageException();
        }
        return value.asString();
    }

    private static int integer(JsonNode node, String field) {
        var value = node.path(field);
        if (!value.isIntegralNumber() || !value.canConvertToInt()) {
            throw new PoisonMessageException();
        }
        return value.intValue();
    }

    private static String utf8(byte[] bytes) {
        if (bytes == null) {
            throw new PoisonMessageException();
        }
        try {
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException ex) {
            throw new PoisonMessageException();
        }
    }
}
