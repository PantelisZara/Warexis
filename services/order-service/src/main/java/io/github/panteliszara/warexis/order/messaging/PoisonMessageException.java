package io.github.panteliszara.warexis.order.messaging;

public class PoisonMessageException extends RuntimeException {
    public PoisonMessageException() {
        super("Invalid or conflicting domain event.");
    }
}
