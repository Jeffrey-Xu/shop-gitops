import Fastify from 'fastify';
import { config, validateConfig } from './config.js';
import { logger } from './log.js';
import { registry } from './metrics.js';
import { initDb, findShipment, listShipments, pool } from './db.js';
import { OrderPaidConsumer, handleOrderPaid } from './consumer.js';
import { startDispatcher, advanceOne } from './dispatcher.js';

const app = Fastify({
  loggerInstance: logger,
  // Fastify 自带的 req/res 日志字段和我们自己的不一致，
  // 关掉它，只保留业务日志（access log 由 httpx 那套思路手动记）
  disableRequestLogging: true,
  bodyLimit: 64 * 1024,
});

// consumer 在 main() 里创建，readyz 需要读它的连接状态。
// 用模块级变量而不是闭包捕获：路由注册在 main() 之前。
let consumer = null;

// ---- 探针 ----

app.get('/healthz', async () => ({
  status: 'ok',
  service: 'fulfillment',
  version: config.version,
}));

app.get('/readyz', async (req, reply) => {
  try {
    await pool.query('SELECT 1');
  } catch (err) {
    reply.code(503);
    return { ready: false, db: err.message };
  }
  // 消费连接断了就不该再分流量：此时新订单不会建发货单
  if (!consumer?.connected) {
    reply.code(503);
    return { ready: false, rabbitmq: 'disconnected' };
  }
  return { ready: true };
});

app.get('/metrics', async (req, reply) => {
  reply.header('Content-Type', registry.contentType);
  return registry.metrics();
});

// ---- 查询 ----

app.get('/shipments', async () => ({ items: await listShipments(50) }));

app.get('/shipments/:orderId', async (req, reply) => {
  const orderId = Number(req.params.orderId);
  if (!Number.isFinite(orderId)) {
    reply.code(400);
    return { error: 'orderId 非法' };
  }
  const s = await findShipment(orderId);
  if (!s) {
    reply.code(404);
    return { error: '发货单不存在' };
  }
  return s;
});

// ---- 演示用：立刻推进一步，不用等 ticker ----

app.post('/shipments/:orderId/advance', async (req, reply) => {
  const orderId = Number(req.params.orderId);
  const s = await findShipment(orderId);
  if (!s) {
    reply.code(404);
    return { error: '发货单不存在' };
  }
  if (s.status === 'DELIVERED') {
    reply.code(409);
    return { error: '已经签收', status: s.status };
  }
  const row = await advanceOne(s);
  if (!row) {
    // 要么被别人抢了，要么回写失败已经回滚
    const cur = await findShipment(orderId);
    reply.code(409);
    return { error: '本次推进未生效（并发或回写失败）', status: cur?.status };
  }
  return { ok: true, status: row.status, trackingNo: row.tracking_no };
});

// 访问日志：只记非探针请求，字段和 Go 侧 http_request 对齐
app.addHook('onResponse', async (req, reply) => {
  if (['/metrics', '/healthz', '/readyz'].includes(req.url)) return;
  logger.info(
    {
      method: req.method,
      path: req.url.split('?')[0],
      status: reply.statusCode,
      dur_ms: Math.round(reply.elapsedTime),
    },
    'http_request',
  );
});

async function main() {
  const missing = validateConfig();
  if (missing.length > 0) {
    logger.error({ missing }, 'config_invalid');
    process.exit(1);
  }

  await initDb();

  consumer = new OrderPaidConsumer();
  // 不 await：consume 是长驻循环，await 会卡住启动。
  // 消费失败不影响 HTTP 端口起来（但 readyz 会报 503）。
  consumer.start(handleOrderPaid).catch((err) => {
    logger.error({ err: err.message }, 'consumer_crashed');
  });

  const stopDispatcher = startDispatcher();

  await app.listen({ port: config.port, host: '0.0.0.0' });
  logger.info(
    { port: config.port, queue: config.queue, api: config.apiUrl },
    'service_started',
  );

  let shuttingDown = false;
  const shutdown = async (signal) => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info({ signal }, 'shutdown_started');

    // 顺序有讲究：先停止接新单（consume），再停推进，最后关连接池。
    // 反过来会出现「消息 ack 了但还在处理」的窗口。
    stopDispatcher();
    await app.close().catch(() => {});
    await consumer.stop().catch(() => {});
    await pool.end().catch(() => {});
    logger.info({}, 'service_stopped');
    process.exit(0);
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
  // 未捕获异常不能吞：吞掉之后进程还在但状态已经脏了，
  // 让它退出、让 K8s 重启一个干净的实例。
  process.on('unhandledRejection', (err) => {
    logger.error({ err: String(err) }, 'unhandled_rejection');
    shutdown('unhandledRejection');
  });
}

main().catch((err) => {
  logger.error({ err: err.message, stack: err.stack }, 'startup_failed');
  process.exit(1);
});
