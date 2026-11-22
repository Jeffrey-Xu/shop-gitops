import amqp from 'amqplib';
import { context, propagation, SpanKind, SpanStatusCode, trace } from '@opentelemetry/api';
import { config } from './config.js';
import { logger } from './log.js';
import { consumerUp, shipmentsCreated } from './metrics.js';
import { createShipment } from './db.js';
import { pickCarrier, trackingNumber } from './carrier.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// getTracer 在 provider 就绪前调用也没关系：api 包里的 tracer 是个代理，
// 真正 startSpan 时才解析到全局 provider。
const tracer = trace.getTracer('shop-fulfillment');

/**
 * order.paid 的消费者。
 *
 * 与 Go 侧 worker 的分工：
 *   worker      负责「钱」—— 扣款成功/拒付/重试/补偿
 *   fulfillment 负责「货」—— 收钱之后建发货单、推进物流
 *
 * 两者只通过 order.paid 这条消息耦合，任何一边重启都不影响另一边。
 */
export class OrderPaidConsumer {
  #conn = null;
  #ch = null;
  #pub = null;
  #closing = false;
  // 给 readyz 用。刻意不去读 prom-client 的 gauge：
  // 指标是给 Prometheus 的，业务判断反过来依赖指标库的内部结构，
  // 一升级就崩（这里就踩过一次）。
  #connected = false;

  get connected() {
    return this.#connected;
  }

  async start(handler) {
    // 断线重连放在这一层：amqplib 不会自己恢复 channel，
    // 一旦连接断了消费就彻底停了，必须显式重建。
    while (!this.#closing) {
      try {
        await this.#run(handler);
      } catch (err) {
        logger.error({ err: err.message }, 'consumer_loop_error');
      }
      this.#connected = false;
      consumerUp.set(0);
      if (this.#closing) break;
      await sleep(3000);
    }
    logger.info({}, 'consumer_stopped');
  }

  async #run(handler) {
    this.#conn = await amqp.connect(config.rabbitUrl);
    this.#conn.on('error', (err) => {
      // 不接 error 事件，Node 会因为未处理的 EventEmitter error 直接崩
      logger.warn({ err: err.message }, 'rabbit_connection_error');
    });
    const closed = new Promise((resolve) => this.#conn.once('close', resolve));

    this.#ch = await this.#conn.createChannel();
    // 回写用的独立 channel：消费 channel 的 prefetch / ack 状态
    // 不该被 publish 的错误连累
    this.#pub = await this.#conn.createChannel();
    await this.#ch.prefetch(config.prefetch);

    await this.#waitForQueue();
    await this.#ch.consume(config.queue, (msg) => this.#onMessage(msg, handler), {
      noAck: false,
    });

    consumerUp.set(1);
    this.#connected = true;
    logger.info(
      { queue: config.queue, prefetch: config.prefetch },
      'consumer_started',
    );

    await closed;
    logger.warn({}, 'rabbit_connection_closed');
  }

  /**
   * 队列由 Go 侧的 broker.Declare() 创建。
   * 这里只 checkQueue（passive），绝不 declare ——
   * 两边用不同参数声明同一个队列，RabbitMQ 会以 PRECONDITION_FAILED
   * 直接关掉连接，报错信息还只说 "inequivalent arg"，极难排查。
   */
  async #waitForQueue() {
    for (let attempt = 1; attempt <= 30; attempt += 1) {
      if (this.#closing) return;
      try {
        await this.#ch.checkQueue(config.queue);
        return;
      } catch (err) {
        if (err.code !== 404) throw err;
        logger.warn({ attempt, queue: config.queue }, 'queue_not_ready_waiting');
        await sleep(2000);
        // channel 被 404 关掉后必须重建
        this.#ch = await this.#conn.createChannel();
        await this.#ch.prefetch(config.prefetch);
      }
    }
    throw new Error(`队列 ${config.queue} 一直不存在，检查 api 的 broker.Declare 是否跑过`);
  }

  async #onMessage(msg, handler) {
    if (!msg) return;
    const attempts = Number(msg.properties.headers?.attempts ?? 0);

    // 从消息头还原上游 context（traceparent 由 Go 的 broker 注入）。
    // extract 在拿不到头时返回原 ctx，所以手工投的测试消息也能正常跑，
    // 只是它自己会成为一条新 trace 的起点。
    const parent = propagation.extract(context.active(), msg.properties.headers ?? {});
    const span = tracer.startSpan(
      `consume ${config.queue}`,
      {
        kind: SpanKind.CONSUMER,
        attributes: {
          'messaging.system': 'rabbitmq',
          'messaging.operation': 'process',
          'messaging.destination.name': config.queue,
          'messaging.message.receive_attempt': attempts + 1,
        },
      },
      parent,
    );

    try {
      let event;
      try {
        event = JSON.parse(msg.content.toString());
      } catch (err) {
        // 解不开的消息重试一万次也解不开，直接 ACK 丢弃。
        // 真实项目应该落到一张 poison_messages 表里留证据。
        span.setStatus({ code: SpanStatusCode.ERROR, message: 'bad_event_payload' });
        logger.error({ err: err.message, raw: msg.content.toString().slice(0, 200) }, 'bad_event_payload');
        this.#ack(msg);
        return;
      }

      const orderId = event.orderId;
      // 订单号挂在 span 上：Tempo 里能按订单号搜，排障时手上通常只有订单号
      span.setAttribute('order.id', Number(orderId) || 0);

      // context.with 让 handler 内部（包括 await 之后）都看得到这个 span，
      // 这样 undici 的自动埋点才会把回写 api 的 fetch span 挂到它下面。
      // 不包这一层，fetch 的 span 会变成没有父节点的孤儿。
      await context.with(trace.setSpan(parent, span), async () => {
        try {
          await handler(event, attempts);
          this.#ack(msg);
        } catch (err) {
          span.recordException(err);
          span.setStatus({ code: SpanStatusCode.ERROR, message: err.message });
          logger.error(
            { orderId, attempt: attempts + 1, err: err.message },
            'fulfill_failed',
          );
          await this.#retryOrDrop(msg, event, attempts, err);
        }
      });
    } finally {
      span.end();
    }
  }

  #ack(msg) {
    try {
      this.#ch.ack(msg);
    } catch (err) {
      logger.warn({ err: err.message }, 'ack_failed');
    }
  }

  /**
   * 重试走延迟队列（TTL 10s 后死信回主队列），不是 nack+requeue。
   * requeue 会立刻重新入队，三次重试在几毫秒内跑完，等于没有退避。
   */
  async #retryOrDrop(msg, event, attempts, err) {
    const next = attempts + 1;
    const headers = {
      attempts: next,
      last_error: String(err.message ?? err).slice(0, 500),
    };
    // 重试消息也要带 traceparent。不带的话，第一次失败之后的链路全断，
    // 而排查失败订单最需要的恰恰是「重试那几次到底怎么了」。
    // 注入后重试的 consume 会挂在本次 consume 下面，一条 trace 看全四次尝试。
    propagation.inject(context.active(), headers);

    const target =
      next >= config.maxAttempts
        ? { route: config.dlqRoute, label: 'dlq' }
        : { route: config.retryRoute, label: 'retry' };

    try {
      this.#pub.publish(config.retryExchange, target.route, msg.content, {
        contentType: 'application/json',
        persistent: true,
        headers,
      });
      logger.warn(
        { orderId: event.orderId, attempt: next, next: target.label },
        'fulfill_retry_scheduled',
      );
      this.#ack(msg);
      if (target.label === 'dlq') {
        shipmentsCreated.inc({ result: 'error' });
      }
    } catch (pubErr) {
      // 发不出去就把原消息退回队列，不能既没发新消息又 ack 掉原消息
      logger.error({ orderId: event.orderId, err: pubErr.message }, 'republish_failed');
      try {
        this.#ch.nack(msg, false, true);
      } catch (nackErr) {
        logger.error({ err: nackErr.message }, 'nack_failed');
      }
    }
  }

  async stop() {
    this.#closing = true;
    this.#connected = false;
    try {
      if (this.#ch) await this.#ch.close();
      if (this.#pub) await this.#pub.close();
      if (this.#conn) await this.#conn.close();
    } catch (err) {
      logger.warn({ err: err.message }, 'consumer_close_error');
    }
  }
}

/**
 * 消息处理的正体：建发货单。
 *
 * 只做「建单」，不在这里推进物流 —— 推进是定时器的事。
 * 消息处理必须快（毫秒级），任何慢动作都会把 prefetch 占满，
 * 让整个队列停住。
 */
export async function handleOrderPaid(event) {
  const orderId = Number(event.orderId);
  if (!Number.isFinite(orderId) || orderId <= 0) {
    throw new Error(`非法的 orderId: ${event.orderId}`);
  }

  const carrier = pickCarrier(orderId);
  const trackingNo = trackingNumber(carrier, orderId);

  const row = await createShipment({
    orderId,
    userId: event.userId,
    carrier: carrier.name,
    trackingNo,
    items: event.items ?? [],
  });

  if (!row) {
    // 重复投递。这是常态（at-least-once），不算错误。
    shipmentsCreated.inc({ result: 'duplicate' });
    logger.info({ orderId }, 'shipment_duplicate_skipped');
    return;
  }

  shipmentsCreated.inc({ result: 'created' });
  logger.info(
    { orderId, carrier: carrier.name, trackingNo, userId: event.userId },
    'shipment_created',
  );
}
