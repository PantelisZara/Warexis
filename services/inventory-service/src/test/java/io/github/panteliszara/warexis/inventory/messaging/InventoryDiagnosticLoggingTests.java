package io.github.panteliszara.warexis.inventory.messaging;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import tools.jackson.databind.json.JsonMapper;

import io.github.panteliszara.warexis.inventory.reservations.application.InventoryReservationService;
import io.github.panteliszara.warexis.inventory.reservations.domain.ReservationLine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(OutputCaptureExtension.class)
class InventoryDiagnosticLoggingTests {
    @Test
    @SuppressWarnings("unchecked")
    void retryAndDltLogsExposeValidatedIdentityAndBudgetWithoutPayloadOrExceptionText(CapturedOutput output) {
        var eventId = UUID.randomUUID();
        var orderId = UUID.randomUUID();
        var unsafe = "UNSAFE-PAYLOAD-AND-EXCEPTION-TEXT";
        var mapper = JsonMapper.builder().build();
        var envelope = mapper.createObjectNode();
        envelope.put("eventId", eventId.toString()); envelope.put("eventType", "OrderPlaced");
        envelope.put("eventVersion", 1); envelope.put("occurredAt", "2026-10-07T00:00:00Z");
        envelope.put("aggregateId", orderId.toString()); envelope.put("correlationId", orderId.toString());
        envelope.put("optionalDiagnostic", unsafe);
        var payload = envelope.putObject("payload"); payload.put("orderId", orderId.toString());
        payload.set("lines", mapper.valueToTree(List.of(new ReservationLine(UUID.randomUUID(), "A", 1))));
        var record = new ConsumerRecord<byte[], byte[]>("warexis.order.events.v1", 1, 73,
                orderId.toString().getBytes(StandardCharsets.UTF_8), mapper.writeValueAsBytes(envelope));
        var store = mock(ConsumerDeliveryStore.class);
        when(store.load(record)).thenReturn(state("PROCESS", 3, 0, null), state("PROCESS", 4, 0, null),
                state("DLT", 4, 0, "RETRY_EXHAUSTED"), state("COMPLETE", 4, 1, "RETRY_EXHAUSTED"));
        when(store.beginAttempt(any(), any(), anyBoolean())).thenReturn(true);
        when(store.transition(any(), any(), any())).thenReturn(true);
        var business = mock(InventoryReservationService.class);
        when(business.process(any())).thenThrow(new IllegalStateException(unsafe));
        KafkaTemplate<byte[], byte[]> kafka = mock(KafkaTemplate.class);
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        var ack = mock(Acknowledgment.class);
        new InventoryOrderListener(store, new WorkflowEventValidator(mapper), business, kafka)
                .receive(record, ack, mock(Consumer.class));
        verify(business, times(1)).process(any());
        verify(ack).acknowledge();
        assertThat(output.getOut()).contains("eventId=" + eventId, "eventType=OrderPlaced", "orderId=" + orderId,
                "partition=1", "offset=73", "processingAttempt=4", "state=PROCESS_RETRY", "state=DLT",
                "classification=RETRY_EXHAUSTED", "dltAttempt=1", "state=DLT_ACKNOWLEDGED");
        assertThat(output.getAll()).doesNotContain(unsafe);
    }

    @Test
    @SuppressWarnings("unchecked")
    void poisonDiagnosticsUseCoordinatesRatherThanUnvalidatedIdentityOrRawBytes(CapturedOutput output) {
        var unsafe = "UNSAFE-POISON-KEY-AND-PAYLOAD";
        var record = new ConsumerRecord<byte[], byte[]>("warexis.order.events.v1", 2, 91,
                unsafe.getBytes(StandardCharsets.UTF_8), unsafe.getBytes(StandardCharsets.UTF_8));
        var store = mock(ConsumerDeliveryStore.class);
        when(store.load(record)).thenReturn(state("PROCESS", 0, 0, null), state("DLT", 0, 0, "INVALID_CONTRACT"),
                state("COMPLETE", 0, 1, "INVALID_CONTRACT"));
        when(store.beginAttempt(any(), any(), anyBoolean())).thenReturn(true);
        when(store.transition(any(), any(), any())).thenReturn(true);
        KafkaTemplate<byte[], byte[]> kafka = mock(KafkaTemplate.class);
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        var business = mock(InventoryReservationService.class);
        var ack = mock(Acknowledgment.class);
        new InventoryOrderListener(store, new WorkflowEventValidator(JsonMapper.builder().build()), business, kafka)
                .receive(record, ack, mock(Consumer.class));
        verifyNoInteractions(business);
        verify(ack).acknowledge();
        assertThat(output.getOut()).contains("eventId=null", "eventType=null", "orderId=null", "partition=2",
                "offset=91", "state=DLT", "classification=INVALID_CONTRACT", "dltAttempt=1");
        assertThat(output.getAll()).doesNotContain(unsafe);
    }

    private ConsumerDeliveryStore.State state(String phase, int process, int dlt, String code) {
        return new ConsumerDeliveryStore.State(phase, process, dlt, null, code);
    }
}
