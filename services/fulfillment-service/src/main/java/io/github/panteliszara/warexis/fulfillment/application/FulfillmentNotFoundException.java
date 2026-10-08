package io.github.panteliszara.warexis.fulfillment.application;

public class FulfillmentNotFoundException extends RuntimeException {
    public FulfillmentNotFoundException() {
        super("Fulfillment not found.");
    }
}
