package io.github.panteliszara.warexis.fulfillment.application;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;
import io.github.panteliszara.warexis.fulfillment.messaging.Confirmation;
import io.github.panteliszara.warexis.fulfillment.messaging.EventJson;
import io.github.panteliszara.warexis.fulfillment.messaging.PoisonMessageException;

@Service
public class FulfillmentService {
    private final JdbcClient jdbc;
    private final JsonMapper json;
    public FulfillmentService(JdbcClient jdbc, JsonMapper json) {
        this.jdbc = jdbc;
        this.json = json;
    }

    @Transactional
    public boolean accept(Confirmation input) {
        // Revalidate internal inputs through the same closed transport contract.
        var canonical = json.writeValueAsString(inputEnvelope(input));
        EventJson.confirmation(input.orderId().toString().getBytes(java.nio.charset.StandardCharsets.UTF_8),
                canonical.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        if (!input.eventType().equals("OrderConfirmed")) {
            return false;
        }
        lock(601,input.eventId().toString());
        lock(602,input.orderId().toString());
        var previous=jdbc.sql("SELECT event_json::text FROM processed_confirmations WHERE event_id=:id")
                .param("id",input.eventId()).query(String.class).optional();
        if (previous.isPresent()) {
            if (!json.readTree(previous.get()).equals(json.readTree(canonical))) {
                throw new PoisonMessageException();
            }
            return false;
        }
        var owned=jdbc.sql("SELECT id FROM fulfillments WHERE order_id=:order")
                .param("order",input.orderId()).query(UUID.class).optional();
        UUID id;
        if (owned.isPresent()) {
            id = owned.get();
            var view = load(id);
            var cause=jdbc.sql("SELECT confirmation_causation_id FROM fulfillments WHERE id=:id").param("id",id).query(UUID.class).single();
            if (!view.reservationId().equals(input.reservationId()) || !view.lines().equals(input.lines())
                    || !cause.equals(input.causationId())) {
                throw new PoisonMessageException();
            }
        } else {
            // Serialize shared reservation ownership to classify conflicts before DB uniqueness.
            lock(603,input.reservationId().toString());
            if (jdbc.sql("SELECT COUNT(*) FROM fulfillments WHERE reservation_id=:id")
                    .param("id",input.reservationId()).query(Long.class).single() > 0) {
                throw new PoisonMessageException();
            }
            id = UUID.randomUUID();
            var now = Instant.now().truncatedTo(ChronoUnit.MICROS);
            requireOne(jdbc.sql("""
                    INSERT INTO fulfillments(id,order_id,reservation_id,confirmation_event_id,confirmation_causation_id,status,created_at,updated_at)
                    VALUES(:id,:order,:reservation,:event,:cause,'PENDING',:now,:now)
                    """).param("id",id).param("order",input.orderId()).param("reservation",input.reservationId())
                    .param("event",input.eventId()).param("cause",input.causationId()).param("now",Timestamp.from(now)).update());
            for (int n=0;n<input.lines().size();n++) {
                var line=input.lines().get(n);
                requireOne(jdbc.sql("INSERT INTO fulfillment_lines VALUES(:id,:line,:position,:sku,:quantity)")
                        .param("id",id).param("line",line.orderLineId()).param("position",n)
                        .param("sku",line.sku()).param("quantity",line.quantity()).update());
            }
        }
        requireOne(jdbc.sql("INSERT INTO processed_confirmations VALUES(:event,:id,CAST(:json AS jsonb),CURRENT_TIMESTAMP)")
                .param("event",input.eventId()).param("id",id).param("json",canonical).update());
        return owned.isEmpty();
    }

    private Map<String, Object> inputEnvelope(Confirmation in) {
        var envelope=new java.util.LinkedHashMap<String, Object>();
        envelope.put("eventId",in.eventId());
        envelope.put("eventType",in.eventType());
        envelope.put("eventVersion",1);
        envelope.put("occurredAt",in.occurredAt());
        envelope.put("aggregateId",in.orderId());
        envelope.put("correlationId",in.orderId());
        var payload = new java.util.LinkedHashMap<String, Object>();
        payload.put("orderId",in.orderId());
        payload.put("lines",in.lines());
        if (in.eventType().equals("OrderConfirmed")) {
            envelope.put("causationId", in.causationId());
            payload.put("reservationId", in.reservationId());
        }
        envelope.put("payload", payload);
        return envelope;
    }

    @Transactional(readOnly = true)
    public FulfillmentView get(UUID id) {
        return load(id);
    }

    @Transactional(readOnly = true)
    public FulfillmentView byOrder(UUID order) {
        return load(jdbc.sql("SELECT id FROM fulfillments WHERE order_id=:order").param("order",order)
                .query(UUID.class).optional().orElseThrow(FulfillmentNotFoundException::new));
    }

    @Transactional
    public FulfillmentView ship(UUID id, String reference) {
        if (reference == null || !reference.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,63}")) {
            throw new IllegalArgumentException("Invalid shipment reference.");
        }
        lock(604,id.toString());
        var view=load(id);
        if (view.status().equals("SHIPPED")) {
            if (!reference.equals(view.shipmentReference())) {
                throw new ShipmentConflictException();
            }
            return view;
        }
        lock(605,reference);
        if (jdbc.sql("SELECT COUNT(*) FROM fulfillments WHERE shipment_reference=:ref").param("ref",reference)
                .query(Long.class).single() > 0) {
            throw new ShipmentConflictException();
        }
        var now=Instant.now().truncatedTo(ChronoUnit.MICROS);
        if (now.isBefore(view.createdAt())) {
            now = view.createdAt();
        }
        requireOne(jdbc.sql("UPDATE fulfillments SET status='SHIPPED',shipped_at=:now,updated_at=:now,shipment_reference=:ref WHERE id=:id")
                .param("now",Timestamp.from(now)).param("ref",reference).param("id",id).update());
        var eventId=UUID.randomUUID();
        var payload=Map.of("fulfillmentId",id,"orderId",view.orderId(),"reservationId",view.reservationId(),
                "orderConfirmedEventId",view.confirmationEventId(),"shipmentReference",reference,"shippedAt",now,"lines",view.lines());
        var envelope=Map.of("eventId",eventId,"eventType","FulfillmentShipped","eventVersion",1,"occurredAt",now,
                "aggregateId",id,"correlationId",view.orderId(),"payload",payload);
        requireOne(jdbc.sql("""
                INSERT INTO outbox_events(event_id,aggregate_id,event_type,event_version,occurred_at,correlation_id,event_json)
                VALUES(:event,:id,'FulfillmentShipped',1,:now,:order,CAST(:json AS jsonb))
                """).param("event",eventId).param("id",id).param("now",Timestamp.from(now)).param("order",view.orderId())
                .param("json",json.writeValueAsString(envelope)).update());
        return load(id);
    }

    private FulfillmentView load(UUID id) {
        var lines=jdbc.sql("SELECT order_line_id,sku,quantity FROM fulfillment_lines WHERE fulfillment_id=:id ORDER BY line_position")
                .param("id",id).query((r,n)->new Confirmation.Line(r.getObject(1,UUID.class),r.getString(2),r.getInt(3))).list();
        return jdbc.sql("SELECT * FROM fulfillments WHERE id=:id").param("id",id).query((r,n)->new FulfillmentView(id,
                r.getObject("order_id",UUID.class),r.getObject("reservation_id",UUID.class),r.getObject("confirmation_event_id",UUID.class),
                r.getString("status"),r.getTimestamp("created_at").toInstant(),r.getTimestamp("updated_at").toInstant(),
                r.getTimestamp("shipped_at")==null?null:r.getTimestamp("shipped_at").toInstant(),r.getString("shipment_reference"),List.copyOf(lines)))
                .optional().orElseThrow(FulfillmentNotFoundException::new);
    }

    private void requireOne(int rows) {
        if (rows != 1) {
            throw new IllegalStateException("Required persistence change was not recorded.");
        }
    }

    private void lock(int namespace,String identity) {
        jdbc.sql("SELECT pg_advisory_xact_lock(:ns,hashtext(:identity))").param("ns",namespace).param("identity",identity)
                .query((r,n)->true).single();
    }
}
