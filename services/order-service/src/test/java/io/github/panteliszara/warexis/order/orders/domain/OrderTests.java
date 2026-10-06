package io.github.panteliszara.warexis.order.orders.domain;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class OrderTests {

    @Test
    void creationPreservesRepeatedSkuLinesAndAssignsInitialStateAndDistinctIds() {
        var now = Instant.parse("2026-10-06T10:00:00.123456789Z");
        var order = Order.create(List.of(new OrderLineInput("Abc-1", 2), new OrderLineInput("Abc-1", 3)), now);

        assertThat(order.getId()).isNotNull();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.PENDING_INVENTORY);
        assertThat(order.getCreatedAt()).isEqualTo(Instant.parse("2026-10-06T10:00:00.123456Z"));
        assertThat(order.getUpdatedAt()).isEqualTo(order.getCreatedAt());
        assertThat(order.getLines()).extracting(OrderLine::getSku).containsExactly("Abc-1", "Abc-1");
        assertThat(order.getLines()).extracting(OrderLine::getQuantity).containsExactly(2, 3);
        assertThat(order.getLines()).extracting(OrderLine::getId).doesNotHaveDuplicates().doesNotContainNull();
        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> order.getLines().clear());
    }

    @Test
    void rejectsMissingEmptyNullAndExcessiveLines() {
        assertThatIllegalArgumentException().isThrownBy(() -> Order.create(null, Instant.now()));
        assertThatIllegalArgumentException().isThrownBy(() -> Order.create(List.of(), Instant.now()));
        assertThatIllegalArgumentException().isThrownBy(() -> Order.create(Arrays.asList((OrderLineInput) null), Instant.now()));
        var lines = IntStream.range(0, 101).mapToObj(i -> new OrderLineInput("SKU", 1)).toList();
        assertThatIllegalArgumentException().isThrownBy(() -> Order.create(lines, Instant.now()));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" space", "trailing ", "a/b", "a b", "é", "-ABC", "a\n"})
    void rejectsInvalidSku(String sku) {
        assertThatIllegalArgumentException().isThrownBy(() -> new OrderLineInput(sku, 1));
    }

    @Test
    void enforcesSkuLengthAndPositiveQuantityBoundaries() {
        assertThat(new OrderLineInput("A".repeat(64), Integer.MAX_VALUE).sku()).hasSize(64);
        assertThatIllegalArgumentException().isThrownBy(() -> new OrderLineInput("A".repeat(65), 1));
        assertThatIllegalArgumentException().isThrownBy(() -> new OrderLineInput("SKU", 0));
        assertThatIllegalArgumentException().isThrownBy(() -> new OrderLineInput("SKU", -1));
    }
}
