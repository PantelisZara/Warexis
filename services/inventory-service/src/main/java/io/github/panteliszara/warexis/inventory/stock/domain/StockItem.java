package io.github.panteliszara.warexis.inventory.stock.domain;

import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "stock_items")
public class StockItem {

    public static final String SKU_PATTERN = "[A-Za-z0-9][A-Za-z0-9._-]{0,63}";

    @Id
    private UUID id;

    @Column(nullable = false, unique = true, length = 64, updatable = false)
    private String sku;

    @Column(name = "on_hand", nullable = false)
    private int onHand;

    @Column(nullable = false)
    private int reserved;

    protected StockItem() {
    }

    public static StockItem create(String sku, int onHand) {
        if (sku == null || !sku.matches(SKU_PATTERN)) {
            throw new IllegalArgumentException("SKU must use 1 to 64 ASCII letters, digits, dots, underscores or hyphens, starting with a letter or digit.");
        }
        var item = new StockItem();
        item.id = UUID.randomUUID();
        item.sku = sku;
        item.updateOnHand(onHand);
        return item;
    }

    public void updateOnHand(int onHand) {
        if (onHand < 0) {
            throw new IllegalArgumentException("On-hand quantity must be nonnegative.");
        }
        if (onHand < reserved) {
            throw new StockConflictException("On-hand quantity cannot be less than reserved stock.");
        }
        this.onHand = onHand;
    }

    public void reserve(int quantity) {
        if (quantity <= 0) {
            throw new IllegalArgumentException("Reservation quantity must be positive.");
        }
        if (quantity > getAvailable()) {
            throw new StockConflictException("Insufficient stock for reservation.");
        }
        reserved += quantity;
    }

    public UUID getId() {
        return id;
    }

    public String getSku() {
        return sku;
    }

    public int getOnHand() {
        return onHand;
    }

    public int getReserved() {
        return reserved;
    }

    public int getAvailable() {
        return onHand - reserved;
    }
}
