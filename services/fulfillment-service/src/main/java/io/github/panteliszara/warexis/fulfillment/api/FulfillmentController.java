package io.github.panteliszara.warexis.fulfillment.api;

import java.util.UUID;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.github.panteliszara.warexis.fulfillment.application.FulfillmentService;
import io.github.panteliszara.warexis.fulfillment.application.FulfillmentView;

@RestController
@RequestMapping("/api/fulfillments")
public class FulfillmentController {
    private final FulfillmentService service;

    public FulfillmentController(FulfillmentService service) {
        this.service = service;
    }

    @GetMapping("/{id}")
    public FulfillmentView get(@PathVariable String id) {
        return service.get(identifier(id));
    }

    @GetMapping("/orders/{orderId}")
    public FulfillmentView byOrder(@PathVariable String orderId) {
        return service.byOrder(identifier(orderId));
    }

    @PostMapping("/{id}/ship")
    public FulfillmentView ship(@PathVariable String id, @Valid @RequestBody ShipRequest request) {
        return service.ship(identifier(id), request.shipmentReference());
    }

    public record ShipRequest(
            @NotNull @Pattern(regexp = "[A-Za-z0-9][A-Za-z0-9._-]{0,63}") String shipmentReference) {
    }

    private UUID identifier(String value) {
        var id = UUID.fromString(value);
        if (!id.toString().equals(value)) {
            throw new IllegalArgumentException("Invalid identifier.");
        }
        return id;
    }
}
