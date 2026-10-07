package io.github.panteliszara.warexis.inventory.messaging;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.UUID;

import org.springframework.stereotype.Component;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import io.github.panteliszara.warexis.inventory.reservations.application.OrderPlaced;
import io.github.panteliszara.warexis.inventory.reservations.domain.ReservationLine;

@Component
public class WorkflowEventValidator {
    private final JsonMapper mapper;
    public WorkflowEventValidator(JsonMapper mapper) {
        this.mapper = mapper.rebuild().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).build();
    }

    public record Validated(String eventType, OrderPlaced order) {}

    public Validated validate(byte[] key, byte[] bytes) {
        try {
            var root = mapper.readTree(utf8(bytes));
            if (!root.isObject()) throw new InvalidEventException();
            var eventId = uuid(root, "eventId");
            var type = text(root, "eventType");
            var version = root.path("eventVersion");
            if (!version.isIntegralNumber() || !version.canConvertToInt() || version.asInt() != 1) throw new InvalidEventException();
            var occurredAt = text(root, "occurredAt");
            if (!occurredAt.endsWith("Z")) throw new InvalidEventException();
            Instant.parse(occurredAt);
            var aggregateId = uuid(root, "aggregateId");
            var correlationId = uuid(root, "correlationId");
            var payload = root.path("payload");
            if (!payload.isObject()) throw new InvalidEventException();
            var orderId = uuid(payload, "orderId");
            if (!correlationId.equals(orderId) || !utf8(key).equals(orderId.toString())) throw new InvalidEventException();
            var lines = payload.path("lines");
            if (!lines.isArray() || lines.isEmpty() || lines.size() > 100) throw new InvalidEventException();
            var inputs = new ArrayList<ReservationLine>();
            for (var line : lines) {
                if (!line.isObject()) throw new InvalidEventException();
                var quantity = line.path("quantity");
                if (!quantity.isIntegralNumber() || !quantity.canConvertToInt() || quantity.asInt() <= 0) throw new InvalidEventException();
                inputs.add(new ReservationLine(uuid(line, "orderLineId"), text(line, "sku"), quantity.asInt()));
            }
            var input = new OrderPlaced(eventId, orderId, inputs);
            switch (type) {
                case "OrderPlaced" -> {
                    if (!aggregateId.equals(orderId) || root.has("causationId")) throw new InvalidEventException();
                }
                case "InventoryReserved", "InventoryRejected" -> {
                    if (!aggregateId.equals(uuid(payload, "reservationId"))) throw new InvalidEventException();
                    uuid(root, "causationId");
                    if (type.equals("InventoryReserved")) {
                        if (payload.has("reasonCode")) throw new InvalidEventException();
                    } else {
                        var reason = text(payload, "reasonCode");
                        if (!reason.equals("UNKNOWN_SKU") && !reason.equals("INSUFFICIENT_STOCK")) throw new InvalidEventException();
                    }
                }
                default -> throw new InvalidEventException();
            }
            return new Validated(type, input);
        } catch (InvalidEventException exception) {
            throw exception;
        } catch (RuntimeException exception) {
            throw new InvalidEventException();
        }
    }

    private String text(JsonNode node, String field) {
        var value = node.path(field);
        if (!value.isString()) throw new InvalidEventException();
        return value.asString();
    }

    private UUID uuid(JsonNode node, String field) {
        var text = text(node, field);
        var id = UUID.fromString(text);
        if (!id.toString().equals(text)) throw new InvalidEventException();
        return id;
    }

    private String utf8(byte[] bytes) {
        if (bytes == null) throw new InvalidEventException();
        try {
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException exception) {
            throw new InvalidEventException();
        }
    }
}
