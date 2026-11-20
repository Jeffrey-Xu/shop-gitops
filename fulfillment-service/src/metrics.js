import client from 'prom-client';

// 每个进程一个独立 registry。不用全局默认 registry：
// 测试里重复 import 同一个模块会报 "metric already registered"。
export const registry = new client.Registry();

// 进程级指标（CPU / 堆 / event loop 延迟），名字带 process_ 前缀。
// event loop lag 对 Node 服务特别重要：一个同步阻塞的 JSON.parse
// 就能让所有请求延迟抖一下，而这个指标是唯一能看见它的地方。
client.collectDefaultMetrics({ register: registry, prefix: 'fulfillment_' });

export const shipmentsCreated = new client.Counter({
  name: 'fulfillment_shipments_created_total',
  help: '发货单创建结果',
  labelNames: ['result'], // created / duplicate / error
  registers: [registry],
});

export const advances = new client.Counter({
  name: 'fulfillment_advances_total',
  help: '物流状态推进次数',
  labelNames: ['from', 'to', 'result'], // result: ok / callback_failed
  registers: [registry],
});

export const callbackDuration = new client.Histogram({
  name: 'fulfillment_callback_duration_seconds',
  help: '回写订单状态的耗时',
  labelNames: ['status', 'result'],
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5],
  registers: [registry],
});

export const inFlight = new client.Gauge({
  name: 'fulfillment_orders_in_flight',
  help: '尚未送达的发货单数量（每 tick 采样）',
  registers: [registry],
});

export const consumerUp = new client.Gauge({
  name: 'fulfillment_consumer_connected',
  help: '1 = RabbitMQ 消费连接正常，0 = 断开',
  registers: [registry],
});
