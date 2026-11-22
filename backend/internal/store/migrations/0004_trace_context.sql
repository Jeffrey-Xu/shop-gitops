-- outbox 是定时器驱动的，和下单请求不在同一个调用栈里：
-- relay 每 2 秒捞一批行，那时候下单的 span 早就结束了，context 早就丢了。
--
-- 所以 traceparent 必须跟着消息一起落库，relay 读出来再塞进消息头，
-- 一单的链路才能跨过「事务 → 轮询 → 发布」这三段。
-- 不落库的话，trace 会在这里断成两条：一条止于 POST /api/orders，
-- 另一条从 worker 的 consume 开始，两者之间没有任何关系。
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS traceparent TEXT NOT NULL DEFAULT '';

COMMENT ON COLUMN outbox.traceparent IS '写这一行时的 W3C traceparent，relay 发布时注入消息头';
