-- 0002_fulfillment_auth.sql
-- 两件事：
--   1. 订单状态机往后延长：PAID 之后还有 SHIPPED / DELIVERED（履约服务推进）
--   2. 引入账号体系：users 表 + orders.user_id

-- ---------------------------------------------------------------------------
-- 一、状态机
-- 原来的 CHECK 只有三个值，Node 侧履约服务一回写 SHIPPED 就会撞约束。
-- 先 DROP 再 ADD，不能 ALTER 一个已存在的 CHECK 的取值集合。
-- ---------------------------------------------------------------------------
ALTER TABLE orders DROP CONSTRAINT orders_status_check;

ALTER TABLE orders ADD CONSTRAINT orders_status_check
    CHECK (status IN ('CREATED', 'PAID', 'SHIPPED', 'DELIVERED', 'CANCELLED'));

-- orders(status) 之前没有索引：大盘按状态聚合、对账扫 PAID/SHIPPED 都是全表。
-- 这是之前就欠下的，顺手补上。
CREATE INDEX orders_status_idx ON orders (status, updated_at DESC);

-- 运单号。逻辑上的 owner 是履约服务（shipments 表），
-- 这里冗余一份是为了读路径：GET /api/orders/{id} 不能为了显示一个单号
-- 就去跨服务调一次 HTTP。写入口仍然只有履约服务的回调。
ALTER TABLE orders ADD COLUMN tracking_no TEXT;

-- ---------------------------------------------------------------------------
-- 二、账号
-- email 用表达式唯一索引而不是 CITEXT：
-- 不装扩展、不依赖 collation，lower(email) 一行就够。
-- 代价是 ON CONFLICT 要写成 ON CONFLICT (lower(email))，也是合法的。
-- ---------------------------------------------------------------------------
CREATE TABLE users (
    id            BIGSERIAL PRIMARY KEY,
    email         TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    display_name  TEXT NOT NULL DEFAULT '',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX users_email_lower_key ON users (lower(email));

-- 订单归属人。允许 NULL：匿名下单仍然支持（前端不登录也能买）。
-- ON DELETE SET NULL 而不是 CASCADE：删账号不该把订单历史一起删掉，
-- 订单是要留档的财务数据。
ALTER TABLE orders ADD COLUMN user_id BIGINT REFERENCES users (id) ON DELETE SET NULL;

CREATE INDEX orders_user_id_idx ON orders (user_id, created_at DESC);
