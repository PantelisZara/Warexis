CREATE TABLE orders (
    id UUID PRIMARY KEY,
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT orders_status_check CHECK (status IN ('PENDING_INVENTORY', 'CONFIRMED', 'REJECTED')),
    CONSTRAINT orders_timestamps_check CHECK (updated_at >= created_at)
);

CREATE TABLE order_lines (
    id UUID PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES orders(id),
    line_position INTEGER NOT NULL,
    sku VARCHAR(64) NOT NULL,
    quantity INTEGER NOT NULL,
    CONSTRAINT order_lines_position_check CHECK (line_position >= 0 AND line_position < 100),
    CONSTRAINT order_lines_order_position_unique UNIQUE (order_id, line_position),
    CONSTRAINT order_lines_sku_check CHECK (sku ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$'),
    CONSTRAINT order_lines_quantity_check CHECK (quantity > 0)
);
