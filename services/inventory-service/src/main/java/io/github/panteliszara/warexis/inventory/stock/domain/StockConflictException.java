package io.github.panteliszara.warexis.inventory.stock.domain;

public class StockConflictException extends RuntimeException {

    public StockConflictException(String message) {
        super(message);
    }
}
