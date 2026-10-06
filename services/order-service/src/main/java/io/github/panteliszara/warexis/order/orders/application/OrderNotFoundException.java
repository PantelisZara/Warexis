package io.github.panteliszara.warexis.order.orders.application;

public class OrderNotFoundException extends RuntimeException {

    public OrderNotFoundException() {
        super("Order not found.");
    }
}
