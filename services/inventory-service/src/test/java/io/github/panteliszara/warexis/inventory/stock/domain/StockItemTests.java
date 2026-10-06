package io.github.panteliszara.warexis.inventory.stock.domain;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StockItemTests {

    @Test
    void createsStockWithIdentityAndDerivedAvailability() {
        var item = StockItem.create("SKU_1.a-b", 12);
        assertThat(item.getId()).isNotNull();
        assertThat(item.getSku()).isEqualTo("SKU_1.a-b");
        assertThat(item.getReserved()).isZero();
        assertThat(item.getAvailable()).isEqualTo(12);
        var id = item.getId();
        item.updateOnHand(0);
        assertThat(item.getId()).isEqualTo(id);
        assertThat(item.getAvailable()).isZero();
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" SKU", "SKU ", "-SKU", "é", "A/B", "A B", "A\n"})
    void rejectsInvalidSkuWithoutNormalization(String sku) {
        assertThatIllegalArgumentException().isThrownBy(() -> StockItem.create(sku, 1));
    }

    @Test
    void acceptsMaximumSkuAndRejectsLongerSku() {
        assertThat(StockItem.create("A".repeat(64), 0).getSku()).hasSize(64);
        assertThatIllegalArgumentException().isThrownBy(() -> StockItem.create("A".repeat(65), 0));
    }

    @Test
    void rejectsNegativeStockWithoutChangingExistingState() {
        assertThatIllegalArgumentException().isThrownBy(() -> StockItem.create("SKU", -1));
        var item = StockItem.create("SKU", 3);
        assertThatIllegalArgumentException().isThrownBy(() -> item.updateOnHand(-1));
        assertThat(item.getOnHand()).isEqualTo(3);
    }

    @Test
    void preservesReservedInvariantForExistingStock() {
        var item = StockItem.create("SKU", 10);
        // Simulate an existing persisted reservation without adding a reservation API.
        ReflectionTestUtils.setField(item, "reserved", 7);
        assertThatThrownBy(() -> item.updateOnHand(6)).isInstanceOf(StockConflictException.class);
        assertThat(item.getOnHand()).isEqualTo(10);
        assertThat(item.getAvailable()).isEqualTo(3);
        item.updateOnHand(7);
        assertThat(item.getAvailable()).isZero();
        assertThat(item.getReserved()).isEqualTo(7);
    }
}
