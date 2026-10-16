package io.github.panteliszara.warexis.fulfillment.application;

import java.util.UUID;
import io.github.panteliszara.warexis.fulfillment.observability.TraceSupport;
import org.springframework.stereotype.Service;

@Service
public class DispatchService {
    private final FulfillmentService service;
    private final TraceSupport tracing;

    public DispatchService(FulfillmentService service, TraceSupport tracing) {
        this.service = service;
        this.tracing = tracing;
    }

    public FulfillmentView ship(UUID id, String reference) {
        String origin = null;
        try {
            origin = service.confirmationTraceparent(id);
        } catch (RuntimeException ex) {
            // Optional causal metadata must never prevent the sole business invocation.
        }
        return tracing.dispatch(origin, () -> service.ship(id, reference));
    }
}
