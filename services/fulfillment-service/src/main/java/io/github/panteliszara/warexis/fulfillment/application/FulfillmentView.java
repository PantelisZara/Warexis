package io.github.panteliszara.warexis.fulfillment.application;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import io.github.panteliszara.warexis.fulfillment.messaging.Confirmation.Line;
public record FulfillmentView(UUID id, UUID orderId, UUID reservationId, UUID confirmationEventId,
        String status, Instant createdAt, Instant updatedAt, Instant shippedAt, String shipmentReference, List<Line> lines) {}
