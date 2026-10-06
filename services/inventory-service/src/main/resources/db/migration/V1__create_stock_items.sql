CREATE TABLE stock_items (
    id UUID PRIMARY KEY,
    sku VARCHAR(64) COLLATE "C" NOT NULL,
    on_hand INTEGER NOT NULL,
    reserved INTEGER NOT NULL,
    CONSTRAINT stock_items_sku_unique UNIQUE (sku),
    CONSTRAINT stock_items_sku_valid CHECK (sku ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$'),
    CONSTRAINT stock_items_on_hand_nonnegative CHECK (on_hand >= 0),
    CONSTRAINT stock_items_reserved_nonnegative CHECK (reserved >= 0),
    CONSTRAINT stock_items_reserved_within_on_hand CHECK (reserved <= on_hand)
);
