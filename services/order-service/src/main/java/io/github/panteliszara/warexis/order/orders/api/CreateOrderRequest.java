package io.github.panteliszara.warexis.order.orders.api;

import java.util.List;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

public record CreateOrderRequest(@NotNull @Size(min = 1, max = 100) List<@NotNull @Valid Line> lines) {

    public record Line(
            @NotNull @Pattern(regexp = "[A-Za-z0-9][A-Za-z0-9._-]{0,63}") String sku,
            @NotNull @Positive Integer quantity) {
    }
}
