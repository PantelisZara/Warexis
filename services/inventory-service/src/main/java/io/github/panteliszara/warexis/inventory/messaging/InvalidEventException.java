package io.github.panteliszara.warexis.inventory.messaging;

public class InvalidEventException extends RuntimeException {
    public InvalidEventException() { super("Invalid workflow event contract."); }
}
