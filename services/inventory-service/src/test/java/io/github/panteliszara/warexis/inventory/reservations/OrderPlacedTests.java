package io.github.panteliszara.warexis.inventory.reservations;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import io.github.panteliszara.warexis.inventory.reservations.application.OrderPlaced;
import io.github.panteliszara.warexis.inventory.reservations.domain.ReservationLine;
import io.github.panteliszara.warexis.inventory.stock.domain.StockItem;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderPlacedTests {
    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" A", "A ", "é", "-A", "A/B", "A\n"})
    void rejectsInvalidSku(String sku) {
        assertThatThrownBy(() -> new ReservationLine(UUID.randomUUID(), sku, 1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void validatesIdentityQuantityAndOrderSize() {
        assertThatThrownBy(() -> new ReservationLine(null, "A", 1)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ReservationLine(UUID.randomUUID(), "A", 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ReservationLine(UUID.randomUUID(), "A", -1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OrderPlaced(null, UUID.randomUUID(), List.of(line()))).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new OrderPlaced(UUID.randomUUID(), null, List.of(line()))).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> input(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> input(List.of())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> input(IntStream.range(0, 101).mapToObj(i -> line()).toList())).isInstanceOf(IllegalArgumentException.class);
        var repeated = line();
        assertThatThrownBy(() -> input(List.of(repeated, repeated))).isInstanceOf(IllegalArgumentException.class);
        var nullable = new ArrayList<ReservationLine>();
        nullable.add(null);
        assertThatThrownBy(() -> input(nullable)).isInstanceOf(IllegalArgumentException.class);
        assertThat(input(IntStream.range(0, 100).mapToObj(i -> line()).toList()).lines()).hasSize(100);
        assertThat(new ReservationLine(UUID.randomUUID(), "A".repeat(64), Integer.MAX_VALUE).quantity()).isEqualTo(Integer.MAX_VALUE);
        assertThatThrownBy(() -> new ReservationLine(UUID.randomUUID(), "A".repeat(65), 1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void defensivelyCopiesLinesAndProtectsStockDomainInvariants() {
        var lines = new ArrayList<>(List.of(line()));
        var input = input(lines);
        lines.clear();
        assertThat(input.lines()).hasSize(1);
        var stock = StockItem.create("A", 5);
        stock.reserve(4);
        assertThat(stock.getReserved()).isEqualTo(4);
        assertThatThrownBy(() -> stock.reserve(2)).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> stock.reserve(0)).isInstanceOf(IllegalArgumentException.class);
        assertThat(stock.getReserved()).isEqualTo(4);
    }

    private ReservationLine line() { return new ReservationLine(UUID.randomUUID(), "A", 1); }
    private OrderPlaced input(List<ReservationLine> lines) { return new OrderPlaced(UUID.randomUUID(), UUID.randomUUID(), lines); }
}
