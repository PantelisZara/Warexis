package io.github.panteliszara.warexis.order.orders.api;

import java.net.URI;
import java.util.UUID;

import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import io.github.panteliszara.warexis.order.orders.application.OrderService;
import io.github.panteliszara.warexis.order.orders.application.OrderView;
import io.github.panteliszara.warexis.order.orders.domain.OrderLineInput;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderService orders;

    public OrderController(OrderService orders) {
        this.orders = orders;
    }

    @PostMapping
    public ResponseEntity<OrderView> create(@Valid @RequestBody CreateOrderRequest request) {
        var order = orders.create(request.lines().stream()
                .map(line -> new OrderLineInput(line.sku(), line.quantity())).toList());
        return ResponseEntity.created(URI.create("/api/orders/" + order.id())).body(order);
    }

    @GetMapping("/{id}")
    public OrderView get(@PathVariable String id) {
        UUID orderId;
        try {
            orderId = UUID.fromString(id);
        } catch (IllegalArgumentException exception) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Order ID must be a canonical UUID.");
        }
        if (!orderId.toString().equalsIgnoreCase(id)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Order ID must be a canonical UUID.");
        }
        return orders.get(orderId);
    }
}
