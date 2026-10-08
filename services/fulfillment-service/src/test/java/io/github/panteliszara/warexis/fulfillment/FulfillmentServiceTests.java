package io.github.panteliszara.warexis.fulfillment;
import java.net.URI;
import java.net.http.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.*;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.json.JsonMapper;
import io.github.panteliszara.warexis.fulfillment.application.*;
import io.github.panteliszara.warexis.fulfillment.messaging.*;
import static org.assertj.core.api.Assertions.*;
@SpringBootTest(webEnvironment=SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(FulfillmentServiceTests.PostgresConfiguration.class)
class FulfillmentServiceTests {
    @Autowired FulfillmentService service;
    @Autowired JdbcClient jdbc;
    @Autowired JsonMapper json;
    @Autowired org.flywaydb.core.Flyway flyway;
    @LocalServerPort int port;
    static Confirmation input() { return new Confirmation(UUID.randomUUID(),"OrderConfirmed",Instant.parse("2026-10-07T00:00:00Z"),
            UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),List.of(new Confirmation.Line(UUID.randomUUID(),"Abc-1",2),new Confirmation.Line(UUID.randomUUID(),"Abc-1",3))); }
    static Confirmation alias(Confirmation in) { return new Confirmation(UUID.randomUUID(),in.eventType(),in.occurredAt().plusSeconds(1),in.orderId(),in.reservationId(),in.causationId(),in.lines()); }
    @Test void migrationsAndOwnedDatabaseValidate() {
        assertThat(flyway.validateWithResult().validationSuccessful).isTrue();
        assertThat(jdbc.sql("SELECT COUNT(*) FROM flyway_schema_history WHERE success=true").query(Long.class).single()).isEqualTo(1);
        assertThat(jdbc.sql("SELECT version()").query(String.class).single()).contains("PostgreSQL 18");
    }
    @Test void createsPendingOriginalOrderedSnapshotAndGetsThroughBothRoutes() throws Exception {
        var in=input();service.accept(in);var plan=service.byOrder(in.orderId());
        assertThat(plan.status()).isEqualTo("PENDING");assertThat(plan.lines()).isEqualTo(in.lines());
        assertThat(plan.createdAt()).isEqualTo(plan.updatedAt());assertThat(plan.shippedAt()).isNull();
        assertThat(plan.confirmationEventId()).isEqualTo(in.eventId());
        assertThat(get("/api/fulfillments/"+plan.id()).statusCode()).isEqualTo(200);
        assertThat(get("/api/fulfillments/orders/"+in.orderId()).body()).isEqualTo(get("/api/fulfillments/"+plan.id()).body());
    }
    @Test void manualShipAtomicallyStoresOneFactAndRepeatsOriginalProjection() throws Exception {
        var in=input();service.accept(in);var plan=service.byOrder(in.orderId());var ref="MANUAL-"+UUID.randomUUID();
        var result=post(plan.id(),"{\"shipmentReference\":\""+ref+"\"}");assertThat(result.statusCode()).isEqualTo(200);
        var shipped=service.get(plan.id()); assertThat(shipped.status()).isEqualTo("SHIPPED");
        assertThat(service.ship(plan.id(),ref)).isEqualTo(shipped);
        assertThat(service.ship(plan.id(),ref).lines()).isEqualTo(in.lines());
        var stored=jdbc.sql("SELECT event_json::text FROM outbox_events WHERE aggregate_id=:id").param("id",plan.id()).query(String.class).single();
        EventJson.shipped(in.orderId().toString().getBytes(java.nio.charset.StandardCharsets.UTF_8),stored.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        var fact=json.readTree(stored);assertThat(fact.has("causationId")).isFalse();
        assertThat(fact.path("payload").path("orderConfirmedEventId").asString()).isEqualTo(in.eventId().toString());
        assertThat(fact.path("occurredAt").asString()).isEqualTo(shipped.shippedAt().toString());
        assertThat(count("outbox_events","aggregate_id",plan.id())).isEqualTo(1);
    }
    @Test void matchingAliasAfterShipNeverResetsTerminalStateOrFirstIdentity() {
        var in=input();service.accept(in);var plan=service.byOrder(in.orderId());var shipped=service.ship(plan.id(),"ALIAS-"+UUID.randomUUID());
        service.accept(in);service.accept(alias(in));assertThat(service.get(plan.id())).isEqualTo(shipped);
        assertThat(count("processed_confirmations","fulfillment_id",plan.id())).isEqualTo(2);
    }
    @Test void sameIdChangedKnownContentIsPoisonAndConflictingAliasesCannotMutate() {
        var in=input();service.accept(in);var original=service.byOrder(in.orderId());
        var changed=new Confirmation(in.eventId(),in.eventType(),in.occurredAt().plusSeconds(1),in.orderId(),in.reservationId(),in.causationId(),in.lines());
        assertThatThrownBy(()->service.accept(changed)).isInstanceOf(PoisonMessageException.class);
        for (var bad:List.of(new Confirmation(UUID.randomUUID(),in.eventType(),in.occurredAt(),in.orderId(),UUID.randomUUID(),in.causationId(),in.lines()),
                new Confirmation(UUID.randomUUID(),in.eventType(),in.occurredAt(),in.orderId(),in.reservationId(),UUID.randomUUID(),in.lines()),
                new Confirmation(UUID.randomUUID(),in.eventType(),in.occurredAt(),in.orderId(),in.reservationId(),in.causationId(),List.of(in.lines().getLast(),in.lines().getFirst())))) {
            assertThatThrownBy(()->service.accept(bad)).isInstanceOf(PoisonMessageException.class);
        }
        assertThat(service.byOrder(in.orderId())).isEqualTo(original);
        assertThat(count("processed_confirmations","fulfillment_id",original.id())).isEqualTo(1);
    }
    @Test void reusedReservationAcrossOrdersIsPoison() {
        var in=input();service.accept(in);
        var bad=new Confirmation(UUID.randomUUID(),in.eventType(),in.occurredAt(),UUID.randomUUID(),in.reservationId(),in.causationId(),in.lines());
        assertThatThrownBy(()->service.accept(bad)).isInstanceOf(PoisonMessageException.class);
        assertThat(count("fulfillments","reservation_id",in.reservationId())).isEqualTo(1);
    }
    @RepeatedTest(3) void concurrentSameEventAndMatchingAliasesCreateOnlyOneCompletePlan() throws Exception {
        var in=input();var requests=new ArrayList<Confirmation>();for(int i=0;i<12;i++) requests.add(i<6?in:alias(in));
        parallel(requests.stream().<Callable<Void>>map(x->()->{service.accept(x);return null;}).toList());
        var plan=service.byOrder(in.orderId());assertThat(plan.lines()).isEqualTo(in.lines());
        assertThat(count("fulfillments","order_id",in.orderId())).isEqualTo(1);
        assertThat(count("processed_confirmations","fulfillment_id",plan.id())).isEqualTo(7);
    }
    @RepeatedTest(3) void concurrentIdenticalShipCommandsCreateOnlyOneFact() throws Exception {
        var in=input();service.accept(in);var plan=service.byOrder(in.orderId());var ref="RACE-"+UUID.randomUUID();
        var results=parallel(java.util.stream.IntStream.range(0,12).<Callable<FulfillmentView>>mapToObj(i->()->service.ship(plan.id(),ref)).toList());
        assertThat(results).allMatch(x->x.equals(results.getFirst()));assertThat(count("outbox_events","aggregate_id",plan.id())).isEqualTo(1);
    }
    @Test void differentTerminalReferenceAndReusedReferenceReturn409WithoutMutation() throws Exception {
        var a=input();var b=input();service.accept(a);service.accept(b);var one=service.byOrder(a.orderId());var two=service.byOrder(b.orderId());
        var ref="UNIQUE-"+UUID.randomUUID();var shipped=service.ship(one.id(),ref);
        assertThat(post(one.id(),"{\"shipmentReference\":\"DIFFERENT\"}").statusCode()).isEqualTo(409);
        assertThat(post(two.id(),"{\"shipmentReference\":\""+ref+"\"}").statusCode()).isEqualTo(409);
        assertThat(service.get(one.id())).isEqualTo(shipped);assertThat(service.get(two.id())).isEqualTo(two);
        assertThat(count("outbox_events","aggregate_id",two.id())).isZero();
    }
    @ParameterizedTest @ValueSource(strings={"{}","{\"shipmentReference\":null}","{\"shipmentReference\":42}","{\"shipmentReference\":\"a/b\"}","{\"shipmentReference\":\"\"}","{\"shipmentReference\":\"A\",\"extra\":true}","{\"shipmentReference\":\"A\",\"shipmentReference\":\"B\"}","{\"shipmentReference\":\"A\"} {}","[]","null"})
    void strictMalformedBodiesReturn400(String body) throws Exception { assertThat(post(UUID.randomUUID(),body).statusCode()).isEqualTo(400); }
    @Test void missingAndMalformedIdentifiersAreBounded() throws Exception {
        assertThat(get("/api/fulfillments/"+UUID.randomUUID()).statusCode()).isEqualTo(404);
        assertThat(get("/api/fulfillments/1-1-1-1-1").statusCode()).isEqualTo(400);
        assertThat(get("/api/fulfillments/not-a-uuid").statusCode()).isEqualTo(400);
        assertThat(post(UUID.randomUUID(),"{\"shipmentReference\":\"A\"}").statusCode()).isEqualTo(404);
    }
    @ParameterizedTest @ValueSource(strings={"fulfillments","fulfillment_lines","processed_confirmations"})
    void planningFailureRollsBackEverythingAndIdenticalInputRecovers(String table) {
        var in=input();jdbc.sql("ALTER TABLE "+table+" ADD CONSTRAINT injected_rejection CHECK(FALSE) NOT VALID").update();
        try { assertThatThrownBy(()->service.accept(in)).isInstanceOf(org.springframework.dao.DataAccessException.class); }
        finally { jdbc.sql("ALTER TABLE "+table+" DROP CONSTRAINT injected_rejection").update(); }
        assertThat(count("fulfillments","order_id",in.orderId())).isZero();
        assertThat(count("processed_confirmations","event_id",in.eventId())).isZero();
        service.accept(in);assertThat(service.byOrder(in.orderId()).lines()).isEqualTo(in.lines());
    }
    @ParameterizedTest @ValueSource(strings={"fulfillments","outbox_events"})
    void shipmentAndOutboxFailureRollBackTogetherAndCommandRecovers(String table) {
        var in=input();service.accept(in);var plan=service.byOrder(in.orderId());var ref="ROLLBACK-"+UUID.randomUUID();
        jdbc.sql("ALTER TABLE "+table+" ADD CONSTRAINT injected_ship_rejection CHECK(FALSE) NOT VALID").update();
        try { assertThatThrownBy(()->service.ship(plan.id(),ref)).isInstanceOf(org.springframework.dao.DataAccessException.class); }
        finally { jdbc.sql("ALTER TABLE "+table+" DROP CONSTRAINT injected_ship_rejection").update(); }
        assertThat(service.get(plan.id())).isEqualTo(plan);assertThat(count("outbox_events","aggregate_id",plan.id())).isZero();
        assertThat(service.ship(plan.id(),ref).status()).isEqualTo("SHIPPED");
    }
    @Test void databaseConstraintsRejectInvalidStateLineAndEnvelope() {
        var in=input();service.accept(in);var id=service.byOrder(in.orderId()).id();
        for(var sql:List.of("UPDATE fulfillments SET status='INVALID' WHERE id=:id", "UPDATE fulfillments SET status='SHIPPED' WHERE id=:id",
                "UPDATE fulfillments SET updated_at=created_at-interval '1 second' WHERE id=:id", "UPDATE fulfillment_lines SET quantity=0 WHERE fulfillment_id=:id",
                "UPDATE fulfillment_lines SET line_position=100 WHERE fulfillment_id=:id", "UPDATE fulfillment_lines SET sku='bad/sku' WHERE fulfillment_id=:id")) {
            assertThatThrownBy(()->jdbc.sql(sql).param("id",id).update()).isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
        }
        service.ship(id,"DB-"+UUID.randomUUID());
        assertThatThrownBy(()->jdbc.sql("UPDATE outbox_events SET event_json=event_json-'occurredAt' WHERE aggregate_id=:id").param("id",id).update())
                .isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
    }
    @RepeatedTest(3) void concurrentTwoPlansCannotOwnTheSameReference() throws Exception {
        var a=input();var b=input();service.accept(a);service.accept(b);var one=service.byOrder(a.orderId());var two=service.byOrder(b.orderId());
        var ref="OWNERSHIP-"+UUID.randomUUID();
        var results=parallel(List.<Callable<Boolean>>of(()->tryShip(one.id(),ref),()->tryShip(two.id(),ref)));
        assertThat(results).containsExactlyInAnyOrder(true,false);
        var first=service.get(one.id());var second=service.get(two.id());
        assertThat(List.of(first.status(),second.status())).containsExactlyInAnyOrder("SHIPPED","PENDING");
        var loser=first.status().equals("PENDING")?first:second;assertThat(loser.shipmentReference()).isNull();assertThat(loser.shippedAt()).isNull();
        assertThat(count("outbox_events","aggregate_id",loser.id())).isZero();
    }
    @RepeatedTest(3) void concurrentDifferentReferencesOnOnePlanHaveOneWinner() throws Exception {
        var in=input();service.accept(in);var plan=service.byOrder(in.orderId());
        var results=parallel(List.<Callable<Boolean>>of(()->tryShip(plan.id(),"FIRST-"+UUID.randomUUID()),()->tryShip(plan.id(),"SECOND-"+UUID.randomUUID())));
        assertThat(results).containsExactlyInAnyOrder(true,false);assertThat(count("outbox_events","aggregate_id",plan.id())).isEqualTo(1);
    }
    @Test void missingParentsAndDuplicateOwnedIdentitiesAreDatabaseBackstops() {
        var in=input();service.accept(in);var id=service.byOrder(in.orderId()).id();
        for(var sql:List.of("INSERT INTO fulfillment_lines VALUES(gen_random_uuid(),gen_random_uuid(),0,'A',1)",
                "INSERT INTO processed_confirmations VALUES(gen_random_uuid(),gen_random_uuid(),'{}',CURRENT_TIMESTAMP)",
                "INSERT INTO fulfillments SELECT gen_random_uuid(),order_id,gen_random_uuid(),gen_random_uuid(),gen_random_uuid(),status,created_at,updated_at,shipped_at,shipment_reference FROM fulfillments WHERE id=:id",
                "INSERT INTO fulfillments SELECT gen_random_uuid(),gen_random_uuid(),reservation_id,gen_random_uuid(),gen_random_uuid(),status,created_at,updated_at,shipped_at,shipment_reference FROM fulfillments WHERE id=:id",
                "INSERT INTO fulfillment_lines SELECT fulfillment_id,gen_random_uuid(),line_position,sku,quantity FROM fulfillment_lines WHERE fulfillment_id=:id LIMIT 1")) {
            assertThatThrownBy(()->jdbc.sql(sql).param("id",id).update()).isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
        }
    }
    @Test void suppressedShipUpdateRollsBackAndCannotInsertOutgoingFact() {
        var in=input();service.accept(in);var plan=service.byOrder(in.orderId());
        jdbc.sql("CREATE FUNCTION suppress_ship() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NULL; END'").update();
        jdbc.sql("CREATE TRIGGER suppress_ship BEFORE UPDATE ON fulfillments FOR EACH ROW EXECUTE FUNCTION suppress_ship()").update();
        try { assertThatThrownBy(()->service.ship(plan.id(),"SUPPRESS-"+UUID.randomUUID())).isInstanceOf(IllegalStateException.class); }
        finally { jdbc.sql("DROP TRIGGER suppress_ship ON fulfillments").update();jdbc.sql("DROP FUNCTION suppress_ship()").update(); }
        assertThat(service.get(plan.id())).isEqualTo(plan);assertThat(count("outbox_events","aggregate_id",plan.id())).isZero();
    }
    @ParameterizedTest @ValueSource(strings={"fulfillments","fulfillment_lines","processed_confirmations"})
    void suppressedPlanningInsertFailsClosedAndSameInputRecovers(String table) {
        var in=input();suppressInsert(table);
        try { assertThatThrownBy(()->service.accept(in)).isInstanceOf(IllegalStateException.class); }
        finally { restoreInsert(table); }
        assertThat(count("fulfillments","order_id",in.orderId())).isZero();
        assertThat(count("processed_confirmations","event_id",in.eventId())).isZero();
        service.accept(in);assertThat(service.byOrder(in.orderId()).lines()).isEqualTo(in.lines());
    }
    @Test void suppressedProcessedAliasCannotFalselyCompleteNewIdentity() {
        var in=input();service.accept(in);var before=service.byOrder(in.orderId());var other=alias(in);suppressInsert("processed_confirmations");
        try { assertThatThrownBy(()->service.accept(other)).isInstanceOf(IllegalStateException.class); }
        finally { restoreInsert("processed_confirmations"); }
        assertThat(service.byOrder(in.orderId())).isEqualTo(before);
        assertThat(count("processed_confirmations","event_id",other.eventId())).isZero();
        service.accept(other);assertThat(count("processed_confirmations","event_id",other.eventId())).isEqualTo(1);
    }
    @Test void suppressedShippedOutboxInsertRollsBackStateAndReferenceAndCommandRecovers() {
        var in=input();service.accept(in);var before=service.byOrder(in.orderId());var ref="SUPPRESSED-"+UUID.randomUUID();suppressInsert("outbox_events");
        try { assertThatThrownBy(()->service.ship(before.id(),ref)).isInstanceOf(IllegalStateException.class); }
        finally { restoreInsert("outbox_events"); }
        assertThat(service.get(before.id())).isEqualTo(before);assertThat(count("outbox_events","aggregate_id",before.id())).isZero();
        service.ship(before.id(),ref);assertThat(count("outbox_events","aggregate_id",before.id())).isEqualTo(1);
    }
    private void suppressInsert(String table) {
        jdbc.sql("CREATE FUNCTION suppress_insert() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NULL; END'").update();
        jdbc.sql("CREATE TRIGGER suppress_insert BEFORE INSERT ON "+table+" FOR EACH ROW EXECUTE FUNCTION suppress_insert()").update();
    }
    private void restoreInsert(String table) {
        jdbc.sql("DROP TRIGGER suppress_insert ON "+table).update();jdbc.sql("DROP FUNCTION suppress_insert()").update();
    }
    private boolean tryShip(UUID id,String ref) { try {service.ship(id,ref);return true;} catch(ShipmentConflictException ex) {return false;} }
    private long count(String table,String field,UUID id) { return jdbc.sql("SELECT COUNT(*) FROM "+table+" WHERE "+field+"=:id").param("id",id).query(Long.class).single(); }
    private <T> List<T> parallel(List<Callable<T>> tasks) throws Exception {
        var start=new CountDownLatch(1);
        try(var executor=Executors.newFixedThreadPool(tasks.size())) {
            var futures=tasks.stream().map(t->executor.submit(()->{start.await();return t.call();})).toList();start.countDown();
            var results=new ArrayList<T>();for(var f:futures) results.add(f.get(30,TimeUnit.SECONDS));return results;
        }
    }
    private HttpResponse<String> get(String path) throws Exception {
        try(var client=HttpClient.newHttpClient()) { return client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+port+path)).timeout(Duration.ofSeconds(10)).GET().build(),HttpResponse.BodyHandlers.ofString()); }
    }
    private HttpResponse<String> post(UUID id,String body) throws Exception {
        try(var client=HttpClient.newHttpClient()) { return client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+port+"/api/fulfillments/"+id+"/ship")).timeout(Duration.ofSeconds(10)).header("Content-Type","application/json").POST(HttpRequest.BodyPublishers.ofString(body)).build(),HttpResponse.BodyHandlers.ofString()); }
    }
    @TestConfiguration(proxyBeanMethods=false) static class PostgresConfiguration {
        @Bean @ServiceConnection PostgreSQLContainer postgres() { return new PostgreSQLContainer("postgres:18.6"); }
    }
}
