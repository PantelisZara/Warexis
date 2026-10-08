package io.github.panteliszara.warexis.fulfillment.application;

public class ShipmentConflictException extends RuntimeException {
    public ShipmentConflictException() {
        super("Shipment conflicts with committed state.");
    }
}
