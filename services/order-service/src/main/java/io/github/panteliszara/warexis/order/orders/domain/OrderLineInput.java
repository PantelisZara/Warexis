package io.github.panteliszara.warexis.order.orders.domain;

import java.util.regex.Pattern;

public record OrderLineInput(String sku, int quantity) {

    private static final Pattern SKU_PATTERN = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,63}");

    public OrderLineInput {
        if (sku == null || !SKU_PATTERN.matcher(sku).matches()) {
            throw new IllegalArgumentException("SKU must contain 1 to 64 permitted ASCII characters.");
        }
        if (quantity <= 0) {
            throw new IllegalArgumentException("Quantity must be positive.");
        }
    }
}
