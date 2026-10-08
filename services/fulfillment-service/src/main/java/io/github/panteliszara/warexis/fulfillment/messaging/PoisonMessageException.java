package io.github.panteliszara.warexis.fulfillment.messaging;
public class PoisonMessageException extends RuntimeException {
    public PoisonMessageException() { super("Invalid or conflicting workflow event."); }
}
