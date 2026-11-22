-- 0005_outbox_dead.sql
-- outbox 的「毒消息」需要一个显式终态。
--
-- 原来的三处口径不一致：FetchOutbox 只取 attempts < 20，而
-- CountOutboxPending / OldestOutboxAge 完全不看 attempts。
-- 一条永远发不出去的消息（topic 拼错、payload 超大、交换机被删）
-- 失败 20 次后既不再发送、也不被标记 —— pending 恒定不变、年龄单调增长，
-- ShopOutboxStuckByAge 会永久 firing。告警一直响等于没有告警。
--
-- dead_at 的语义是「已经放弃投递，等人工介入」：
-- 它与 published_at 一样让消息退出活跃集合，区别只在于它需要人看。
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS dead_at TIMESTAMPTZ;

-- 最后一次失败的原因。没有它，一条标死的消息只能告诉你「它死了」，
-- 不能告诉你「为什么死的」—— 那个人还得去翻 already rotated 的日志。
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS last_error TEXT;

COMMENT ON COLUMN outbox.dead_at IS '放弃投递的时间：attempts 用尽且超过宽限期后由 relay 标记';
COMMENT ON COLUMN outbox.last_error IS '最后一次 publish 失败的原因，仅用于排查';

-- 活跃未投递的部分索引要把 dead_at 带进去，否则「还有没有活要干」
-- 仍然要回表判断。原来的索引只过滤 published_at。
DROP INDEX IF EXISTS outbox_pending_idx;

CREATE INDEX outbox_pending_idx ON outbox (id)
    WHERE published_at IS NULL AND dead_at IS NULL;

-- 标死的消息很少，但要能一眼列出来（运维查「有哪些消息被我放弃了」）。
CREATE INDEX outbox_dead_idx ON outbox (dead_at DESC)
    WHERE dead_at IS NOT NULL;

-- 对账用不上 orders_status_idx：四类分类是 OR 出来的，没有 status 的等值条件，
-- 只能全表扫。这条部分索引专门服务「卡住的单」这个访问模式。
CREATE INDEX IF NOT EXISTS orders_stuck_idx ON orders (updated_at)
    WHERE status IN ('CREATED', 'CANCELLED');
