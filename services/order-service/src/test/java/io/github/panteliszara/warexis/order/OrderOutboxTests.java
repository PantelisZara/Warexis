package io.github.panteliszara.warexis.order;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.ObjectMapper;

import io.github.panteliszara.warexis.order.orders.application.OrderService;
import io.github.panteliszara.warexis.order.orders.domain.OrderLineInput;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(OrderServiceApplicationTests.PostgresConfiguration.class)
class OrderOutboxTests {

    @Autowired
    private OrderService orders;

    @Autowired
    private JdbcClient jdbc;

    @Autowired
    private ObjectMapper mapper;

    @Autowired
    private PlatformTransactionManager transactions;

    @Test
    void commitsExactlyOneCompleteOrderPlacedEnvelopeWithOriginalLines() {
        var created = orders.create(List.of(new OrderLineInput("Abc-1", 2),
                new OrderLineInput("Abc-1", 3), new OrderLineInput("abc-1", 1)));
        var rows = jdbc.sql("SELECT * FROM outbox_events WHERE aggregate_id = :id")
                .param("id", created.id()).query().listOfRows();
        assertThat(rows).hasSize(1);
        var row = rows.getFirst();
        var eventId = (UUID) row.get("event_id");
        assertThat(eventId).isNotNull();
        assertThat(row.get("event_type")).isEqualTo("OrderPlaced");
        assertThat(row.get("event_version")).isEqualTo(1);
        assertThat(row.get("correlation_id")).isEqualTo(created.id());
        assertThat(row.get("causation_id")).isNull();
        assertThat(row.get("published_at")).isNull();
        var json = mapper.readTree(row.get("event_json").toString());
        var expected = mapper.readTree("""
                {
                  "eventId":"%s", "eventType":"OrderPlaced", "eventVersion":1,
                  "occurredAt":"%s", "aggregateId":"%s", "correlationId":"%s",
                  "payload":{"orderId":"%s", "lines":[
                    {"orderLineId":"%s", "sku":"Abc-1", "quantity":2},
                    {"orderLineId":"%s", "sku":"Abc-1", "quantity":3},
                    {"orderLineId":"%s", "sku":"abc-1", "quantity":1}
                  ]}
                }
                """.formatted(eventId, created.createdAt(), created.id(), created.id(), created.id(),
                created.lines().get(0).id(), created.lines().get(1).id(), created.lines().get(2).id()));
        assertThat(json).isEqualTo(expected);
        var persistedTime = jdbc.sql("SELECT occurred_at FROM outbox_events WHERE event_id = :id")
                .param("id", eventId).query(java.sql.Timestamp.class).single().toInstant();
        assertThat(persistedTime).isEqualTo(created.createdAt());
        assertThat(Instant.parse(json.path("occurredAt").asString())).isEqualTo(persistedTime);
        assertThat(json.path("occurredAt").asString()).endsWith("Z");
        assertThat(orders.get(created.id())).isEqualTo(created);
    }

    @Test
    void rollsBackFlushedOrderAndLinesWhenOutboxInsertFails() {
        var before = counts();
        // NOT VALID leaves prior rows intact while rejecting new inserts after the order flush.
        jdbc.sql("ALTER TABLE outbox_events ADD CONSTRAINT reject_new_events CHECK (event_type <> 'OrderPlaced') NOT VALID")
                .update();
        try {
            assertThatThrownBy(() -> orders.create(List.of(new OrderLineInput("ROLLBACK", 1))))
                    .isInstanceOf(DataIntegrityViolationException.class);
            assertThat(counts()).isEqualTo(before);
        } finally {
            jdbc.sql("ALTER TABLE outbox_events DROP CONSTRAINT reject_new_events").update();
        }
    }

    @Test
    void rollsBackOrderLinesAndOutboxWhenTransactionFailsAfterAllWrites() {
        var before = counts();
        assertThatThrownBy(() -> new TransactionTemplate(transactions).execute(status -> {
            var order = orders.create(List.of(new OrderLineInput("ROLLBACK-AFTER", 2)));
            assertThat(jdbc.sql("SELECT count(*) FROM outbox_events WHERE aggregate_id = :id")
                    .param("id", order.id()).query(Long.class).single()).isEqualTo(1);
            throw new IllegalStateException("Simulated failure before commit");
        })).isInstanceOf(IllegalStateException.class).hasMessage("Simulated failure before commit");
        assertThat(counts()).isEqualTo(before);
    }

    @Test
    void databasePreventsDuplicateEventIdsAndSecondOrderPlacedForSameOrder() {
        var order = orders.create(List.of(new OrderLineInput("UNIQUE", 1)));
        var eventId = jdbc.sql("SELECT event_id FROM outbox_events WHERE aggregate_id = :id")
                .param("id", order.id()).query(UUID.class).single();
        assertDuplicate(() -> jdbc.sql("""
                INSERT INTO outbox_events SELECT * FROM outbox_events WHERE event_id = :id
                """).param("id", eventId).update());
        assertDuplicate(() -> jdbc.sql("""
                INSERT INTO outbox_events (event_id, aggregate_id, event_type, event_version,
                    occurred_at, correlation_id, event_json)
                SELECT :newId, aggregate_id, event_type, event_version, occurred_at, correlation_id,
                    jsonb_set(event_json, '{eventId}', to_jsonb(CAST(:newId AS text)))
                FROM outbox_events WHERE event_id = :id
                """).param("newId", UUID.randomUUID()).param("id", eventId).update());
        assertThat(jdbc.sql("SELECT count(*) FROM outbox_events WHERE aggregate_id = :id")
                .param("id", order.id()).query(Long.class).single()).isEqualTo(1);
    }

    @Test
    void databaseRequiresConsistentEnvelopeAndValidPublicationMetadata() {
        var order = orders.create(List.of(new OrderLineInput("CONSTRAINTS", 1)));
        for (var assignment : List.of("event_version = 0", "event_type = ''",
                "event_json = '{}'::jsonb", "event_json = '[]'::jsonb",
                "event_json = event_json - 'eventId'",
                "event_json = jsonb_set(event_json, '{eventVersion}', '\"1\"'::jsonb)",
                "event_json = jsonb_set(event_json, '{payload,lines}', '[]'::jsonb)",
                "event_json = event_json #- '{payload,lines}'", "correlation_id = gen_random_uuid()",
                "causation_id = gen_random_uuid()", "aggregate_id = gen_random_uuid()",
                "published_at = occurred_at - interval '1 second'")) {
            assertThatThrownBy(() -> jdbc.sql("UPDATE outbox_events SET " + assignment + " WHERE aggregate_id = :id")
                    .param("id", order.id()).update()).isInstanceOf(DataIntegrityViolationException.class);
        }
        assertThat(jdbc.sql("""
                SELECT indexdef FROM pg_indexes WHERE tablename = 'outbox_events'
                AND indexname = 'outbox_unpublished_poll_idx'
                """).query(String.class).single())
                .contains("(occurred_at, event_id)", "WHERE (published_at IS NULL)");
    }

    private void assertDuplicate(Runnable insert) {
        assertThatThrownBy(insert::run).isInstanceOf(DataIntegrityViolationException.class)
                .satisfies(error -> assertThat(((SQLException) error.getCause()).getSQLState()).isEqualTo("23505"));
    }

    private List<Long> counts() {
        return List.of("orders", "order_lines", "outbox_events").stream()
                .map(table -> jdbc.sql("SELECT count(*) FROM " + table).query(Long.class).single()).toList();
    }
}
