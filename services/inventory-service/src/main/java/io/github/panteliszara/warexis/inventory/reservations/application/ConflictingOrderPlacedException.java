package io.github.panteliszara.warexis.inventory.reservations.application;

/** Conflicting immutable event/order identity; unprocessable input, not a business rejection. */
public class ConflictingOrderPlacedException extends RuntimeException {
    public ConflictingOrderPlacedException() {
        super("OrderPlaced conflicts with an existing event or reservation decision.");
    }
}
