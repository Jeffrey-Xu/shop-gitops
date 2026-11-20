import pg from 'pg';
import { config } from './config.js';
import { logger } from './log.js';

const { Pool } = pg;

// 连接池小一点：api 是横着扩的，但这套系统里一个履约服务 5 条连接就够。
// 池子开太大只会把 PG 的 max_connections 吃光，让别的服务连不上。
export const pool = new Pool({
  connectionString: config.databaseUrl,
  max: 5,
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 3_000,
  // 进程被 kill -9 时残留的查询不能一直占着锁
  statement_timeout: 10_000,
});

pool.on('error', (err) => {
  // 空闲连接被服务端关掉是正常现象（PG 重启、网络抖动），
  // 不接这个事件会让 Node 直接退出。
  logger.error({ err: err.message }, 'pg_idle_error');
});

// shipments 是履约服务自己的表，自己建。
// 和 payment-service 建 payments 表是同一个思路：
// 谁拥有数据谁管 schema，Go 侧的迁移体系不碰它。
const SCHEMA = `
CREATE TABLE IF NOT EXISTS shipments (
    id              BIGSERIAL PRIMARY KEY,
    order_id        BIGINT NOT NULL UNIQUE,
    user_id         BIGINT,
    carrier         TEXT NOT NULL,
    tracking_no     TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('PENDING', 'SHIPPED', 'IN_TRANSIT', 'DELIVERED', 'FAILED')),
    items           JSONB NOT NULL DEFAULT '[]'::jsonb,
    attempts        INT NOT NULL DEFAULT 0,
    last_error      TEXT,
    next_advance_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- 只对「还在路上」的单子建部分索引：已签收的历史数据不该拖慢派工扫描
CREATE INDEX IF NOT EXISTS shipments_due_idx
    ON shipments (next_advance_at) WHERE status <> 'DELIVERED';
`;

export async function initDb() {
  let lastErr;
  for (let attempt = 1; attempt <= 15; attempt += 1) {
    try {
      const c = await pool.connect();
      try {
        await c.query(SCHEMA);
      } finally {
        c.release();
      }
      logger.info({ attempt }, 'db_ready');
      return;
    } catch (err) {
      lastErr = err;
      logger.warn({ attempt, err: err.message }, 'db_connect_retry');
      await new Promise((r) => setTimeout(r, 2000));
    }
  }
  throw new Error(`数据库不可达: ${lastErr?.message}`);
}

/**
 * 创建发货单。幂等的核心就一行 ON CONFLICT DO NOTHING：
 * 消息至少投递一次，同一个 order_id 会被反复看到，
 * 这里只有第一次会真的插入，后面都返回 null（= 重复）。
 *
 * 不用「先 SELECT 再 INSERT」：并发下两个消费者会同时通过检查。
 */
export async function createShipment({ orderId, userId, carrier, trackingNo, items }) {
  const { rows } = await pool.query(
    `INSERT INTO shipments (order_id, user_id, carrier, tracking_no, items, status, next_advance_at)
     VALUES ($1, $2, $3, $4, $5::jsonb, 'PENDING', now())
     ON CONFLICT (order_id) DO NOTHING
     RETURNING id, order_id, tracking_no, status`,
    [orderId, userId || null, carrier, trackingNo, JSON.stringify(items ?? [])],
  );
  return rows[0] ?? null;
}

/**
 * 取出到期待推进的发货单。
 * 用数据库时间而不是进程时间：多副本时两台机器的时钟可能差几百毫秒，
 * 用 now() 至少保证「谁先看到」由同一个时钟决定。
 */
export async function dueShipments(limit = 20) {
  const { rows } = await pool.query(
    `SELECT id, order_id, tracking_no, carrier, status, attempts
       FROM shipments
      WHERE status <> 'DELIVERED' AND next_advance_at <= now()
      ORDER BY next_advance_at
      LIMIT $1`,
    [limit],
  );
  return rows;
}

/**
 * 乐观锁推进：WHERE status = $from 才算成功。
 * 多副本同时扫到同一条时，只有一个能拿到行，另一个 no-op ——
 * 这比 SELECT ... FOR UPDATE 便宜，也不会长时间持锁。
 */
export async function advanceShipment(id, from, to, { stepDelayMs }) {
  const { rows } = await pool.query(
    `UPDATE shipments
        SET status = $1,
            attempts = attempts + 1,
            next_advance_at = now() + ($2 || ' milliseconds')::interval,
            updated_at = now()
      WHERE id = $3 AND status = $4
      RETURNING id, order_id, tracking_no, status`,
    [to, String(stepDelayMs), id, from],
  );
  return rows[0] ?? null;
}

/** 回滚状态推进：回写订单失败时用，让数据库保持「状态未变」。 */
export async function rollbackAdvance(id, from, to, reason) {
  await pool.query(
    `UPDATE shipments
        SET status = $1, last_error = $2, updated_at = now(),
            next_advance_at = now() + interval '5 seconds'
      WHERE id = $3 AND status = $4`,
    [from, reason, id, to],
  );
}

export async function markFailed(id, reason) {
  await pool.query(
    `UPDATE shipments
        SET status = 'FAILED', last_error = $1,
            next_advance_at = now() + interval '1 hour', updated_at = now()
      WHERE id = $2 AND status <> 'DELIVERED'`,
    [reason, id],
  );
}

export async function findShipment(orderId) {
  const { rows } = await pool.query(
    `SELECT id, order_id, user_id, carrier, tracking_no, status, items,
            attempts, last_error, created_at, updated_at
       FROM shipments WHERE order_id = $1`,
    [orderId],
  );
  return rows[0] ?? null;
}

export async function listShipments(limit = 50) {
  const { rows } = await pool.query(
    `SELECT order_id, carrier, tracking_no, status, attempts, updated_at
       FROM shipments ORDER BY id DESC LIMIT $1`,
    [limit],
  );
  return rows;
}

export async function countInFlight() {
  const { rows } = await pool.query(
    `SELECT count(*)::int AS n FROM shipments WHERE status NOT IN ('DELIVERED')`,
  );
  return rows[0].n;
}
