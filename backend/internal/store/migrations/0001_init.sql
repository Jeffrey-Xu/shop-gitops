-- 0001_init.sql
-- 旧的 products / products_orders 是 float64 时代的产物，直接弃掉重来。
-- 本地库里只有 seed 数据，DROP 是安全且必要的（price 类型变了，没法原地改）。

DROP TABLE IF EXISTS products_orders;

DROP TABLE IF EXISTS products;

CREATE TABLE products (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    price_cents BIGINT NOT NULL CHECK (price_cents >= 0),
    stock       INT NOT NULL DEFAULT 0 CHECK (stock >= 0),
    version     INT NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO products (name, price_cents, stock) VALUES
    ('机械键盘', 59900, 20),
    ('27 寸显示器', 189900, 5),
    ('人体工学椅', 239900, 3);

CREATE TABLE orders (
    id              BIGSERIAL PRIMARY KEY,
    status          TEXT NOT NULL CHECK (status IN ('CREATED', 'PAID', 'CANCELLED')),
    total_cents     BIGINT NOT NULL CHECK (total_cents >= 0),
    idempotency_key TEXT UNIQUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    id               BIGSERIAL PRIMARY KEY,
    order_id         BIGINT NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
    product_id       BIGINT NOT NULL,
    name             TEXT NOT NULL,
    quantity         INT NOT NULL CHECK (quantity > 0),
    unit_price_cents BIGINT NOT NULL CHECK (unit_price_cents >= 0)
);

CREATE INDEX order_items_order_id_idx ON order_items (order_id);

-- 状态迁移审计表。它和日志的区别：日志可能被轮转/丢失，
-- 这张表是「这个订单到底发生过什么」的最终证据。
CREATE TABLE order_events (
    id          BIGSERIAL PRIMARY KEY,
    order_id    BIGINT NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
    from_status TEXT NOT NULL,
    to_status   TEXT NOT NULL,
    note        TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (order_id, to_status)
);

-- 事务性 outbox：和订单同一个事务写入，保证「订单进库」和「事件发出」
-- 是同一件事。没有它就只能 commit 之后再发消息，进程一挂消息就丢了。
CREATE TABLE outbox (
    id           BIGSERIAL PRIMARY KEY,
    topic        TEXT NOT NULL,
    payload      JSONB NOT NULL,
    attempts     INT NOT NULL DEFAULT 0,
    published_at TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX outbox_pending_idx ON outbox (id) WHERE published_at IS NULL;
