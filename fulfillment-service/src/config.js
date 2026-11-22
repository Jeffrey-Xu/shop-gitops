// 配置集中在一个文件里，全部来自环境变量。
// 原则和 Go 侧一致：能安全给默认值的给默认值，
// 但「密钥类」没有默认值 —— 空字符串就走不通，而不是悄悄连上一个错的地址。

function int(name, def) {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return def;
  const n = Number.parseInt(raw, 10);
  return Number.isFinite(n) && n > 0 ? n : def;
}

export const config = {
  port: int('PORT', 8080),
  version: process.env.APP_VERSION ?? 'dev',
  logLevel: process.env.LOG_LEVEL ?? 'info',

  databaseUrl:
    process.env.DATABASE_URL ??
    'postgresql://shop:shop@127.0.0.1:5432/shop?sslmode=disable',
  // vhost 是 /shop，必须编码成 %2F
  rabbitUrl:
    process.env.RABBITMQ_URL ??
    'amqp://shop:shoppass@127.0.0.1:5672/%2Fshop',

  // 只消费，不声明拓扑 —— 拓扑的 owner 是 Go 侧 broker.Declare()
  queue: process.env.ORDER_PAID_QUEUE ?? 'shop.order.paid',
  retryExchange: process.env.RETRY_EXCHANGE ?? 'shop.dlx',
  retryRoute: process.env.RETRY_ROUTE ?? 'order.paid.retry',
  dlqRoute: process.env.DLQ_ROUTE ?? 'order.paid.dlq',
  prefetch: int('MQ_PREFETCH', 5),
  maxAttempts: int('FULFILL_MAX_ATTEMPTS', 3),

  // 重试退避：第 n 次重试等 base * 2^n，再乘 [0.5,1.5) 的随机系数。
  // 与 Go 侧 worker.retryDelay 用同一套公式 —— 同一条链路的两个消费者，
  // 重试节奏不该长得不一样（否则「order.created 已经退了 4 秒，
  // order.paid 还在整 10 秒一次」这种差异只能靠翻代码才发现）。
  //
  // 上限不能超过 retry 队列的 x-message-ttl（Go 侧 broker 声明的是 10s）：
  // per-message expiration 比队列 TTL 大时会被 broker 截断，表面生效实际不符。
  retryBaseMs: int('FULFILL_RETRY_BASE_MS', 2000),
  retryMaxDelayMs: int('FULFILL_RETRY_MAX_MS', 10000),

  // 回写订单状态要调 Go 的 api。这里不直连数据库，
  // 因为 orders 表的写入口只能有一个。
  apiUrl: process.env.SHOP_API_URL ?? 'http://api:8080',
  internalToken: process.env.INTERNAL_TOKEN ?? '',
  callbackTimeoutMs: int('CALLBACK_TIMEOUT_MS', 3000),

  // 物流推进的节拍。真实世界的干线运输是天级，
  // 这里压到秒级只是为了让演示能在半分钟里看到完整流转。
  tickMs: int('FULFILL_TICK_MS', 6000),
  stepDelayMs: int('FULFILL_STEP_DELAY_MS', 12000),
};

// internalToken 是回写订单的凭证，没有它服务能起但发不了货。
// 与其运行时每个订单都失败一次，不如启动时就说清楚。
export function validateConfig() {
  const missing = [];
  if (!config.internalToken) missing.push('INTERNAL_TOKEN');
  if (!config.apiUrl) missing.push('SHOP_API_URL');
  return missing;
}
