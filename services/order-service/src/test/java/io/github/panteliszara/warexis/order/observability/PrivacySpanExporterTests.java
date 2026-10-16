package io.github.panteliszara.warexis.order.observability;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.data.*;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class PrivacySpanExporterTests {
    static final String PRIVATE = "PRIVATE_SENTINEL /home/private SELECT payload Bearer";
    @ParameterizedTest @EnumSource(StatusCode.class)
    void immutableReconstructionDropsEveryPrivateFieldAndPreservesRemoteBit(StatusCode status) {
        var exported = new AtomicReference<SpanData>();
        var delegate = mock(SpanExporter.class);
        when(delegate.export(any())).thenAnswer(call -> {
            java.util.Collection<SpanData> batch = call.getArgument(0);
            exported.set(batch.iterator().next()); return CompletableResultCode.ofSuccess();
        });
        var raw = mock(SpanData.class);
        var remote = SpanContext.createFromRemoteParent("12345678901234567890123456789012", "1234567890123456",
                TraceFlags.fromByte((byte) 255), TraceState.builder().put("private", "sentinel").build());
        when(raw.getSpanContext()).thenReturn(remote);
        when(raw.getParentSpanContext()).thenReturn(remote);
        when(raw.getKind()).thenReturn(SpanKind.PRODUCER);
        when(raw.getName()).thenReturn(PRIVATE);
        when(raw.getStatus()).thenReturn(StatusData.create(status, PRIVATE));
        when(raw.getResource()).thenReturn(Resource.create(Attributes.builder().put("private", PRIVATE).build(), PRIVATE));
        when(raw.getInstrumentationScopeInfo()).thenReturn(InstrumentationScopeInfo.builder(PRIVATE).setVersion(PRIVATE).setSchemaUrl(PRIVATE)
                .setAttributes(Attributes.builder().put("private", PRIVATE).build()).build());
        when(raw.getAttributes()).thenReturn(Attributes.builder().put("warexis.outcome", PRIVATE).put("warexis.operation", "outbox.publish")
                .put("warexis.event_type", "OrderPlaced").put("http.request.method", "GET").put("http.response.status_code", 201L)
                .put("db.statement", PRIVATE).put("url.full", PRIVATE).build());
        when(raw.getLinks()).thenReturn(List.of(LinkData.create(remote, Attributes.builder().put("private", PRIVATE).build())));
        when(raw.getEvents()).thenReturn(List.of(EventData.create(1, PRIVATE, Attributes.builder().put("private", PRIVATE).build())));
        assertThat(new PrivacySpanExporter(delegate).export(List.of(raw)).isSuccess()).isTrue();
        var safe = exported.get();
        reset(raw); // Reconstructed span retains no delegate and remains independently readable.
        assertThat(safe.toString()).doesNotContain(PRIVATE, "private", "payload", "Bearer");
        assertThat(safe.getName()).isEqualTo("warexis.operation");
        assertThat(safe.getStatus().getStatusCode()).isEqualTo(status == StatusCode.ERROR ? StatusCode.ERROR : StatusCode.UNSET);
        assertThat(safe.getStatus().getDescription()).isEmpty();
        assertThat(safe.getResource().getAttributes()).isEqualTo(Attributes.builder().put("service.name", "order-service").build());
        assertThat(safe.getResource().getSchemaUrl()).isNull();
        assertThat(safe.getInstrumentationScopeInfo().getName()).isEqualTo("warexis.tracing");
        assertThat(safe.getInstrumentationScopeInfo().getVersion()).isEqualTo("1");
        assertThat(safe.getInstrumentationScopeInfo().getSchemaUrl()).isNull();
        assertThat(safe.getInstrumentationScopeInfo().getAttributes()).isEqualTo(Attributes.empty());
        assertThat(safe.getEvents()).isEmpty();
        assertThat(safe.getSpanContext().isRemote()).isTrue();
        assertThat(safe.getParentSpanContext().isRemote()).isTrue();
        assertThat(safe.getSpanContext().getTraceFlags()).isEqualTo(TraceFlags.getSampled());
        assertThat(safe.getSpanContext().getTraceState()).isEqualTo(TraceState.getDefault());
        assertThat(safe.getLinks().getFirst().getSpanContext().isRemote()).isTrue();
        assertThat(safe.getLinks().getFirst().getSpanContext().getTraceState()).isEqualTo(TraceState.getDefault());
        assertThat(safe.getLinks().getFirst().getAttributes()).isEqualTo(Attributes.empty());
        assertThat(safe.getAttributes()).isEqualTo(Attributes.builder().put("warexis.operation", "outbox.publish").put("warexis.event_type", "OrderPlaced")
                .put("http.request.method", "GET").put("http.response.status_code", 201L).build());
    }

    @Test void sanitizationAndExporterOutagesNeverFallbackToRawData() {
        var delegate = mock(SpanExporter.class);
        var raw = mock(SpanData.class);
        when(raw.getName()).thenThrow(new IllegalStateException(PRIVATE));
        var boundary = new PrivacySpanExporter(delegate);
        assertThat(boundary.export(List.of(raw)).isSuccess()).isFalse();
        verifyNoInteractions(delegate);
        when(delegate.export(any())).thenThrow(new IllegalStateException(PRIVATE));
        assertThat(boundary.export(List.of()).isSuccess()).isFalse();
        when(delegate.flush()).thenThrow(new IllegalStateException(PRIVATE));
        when(delegate.shutdown()).thenThrow(new IllegalStateException(PRIVATE));
        assertThat(boundary.flush().isSuccess()).isFalse(); assertThat(boundary.shutdown().isSuccess()).isFalse();
        verify(delegate, times(1)).export(List.of());
    }
}
