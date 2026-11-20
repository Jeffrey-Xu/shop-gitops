import { config } from './config.js';
import { logger } from './log.js';
import { callbackDuration } from './metrics.js';

// 回调失败分两类，处理方式完全不同：
//   retryable = true   → 网络抖动 / 5xx / 超时，等下一轮再来
//   retryable = false  → 4xx，说明请求本身有问题，重试一万次也一样
export class CallbackError extends Error {
  constructor(message, { retryable, status }) {
    super(message);
    this.name = 'CallbackError';
    this.retryable = retryable;
    this.status = status;
  }
}

/**
 * 把发货进度回写给 Go 的 api。
 *
 * 为什么走 HTTP 而不是直接 UPDATE orders 表：
 * 共享数据库看起来最省事，但它把两个服务的 schema 焊死在一起 ——
 * Go 侧改个列名，Node 侧就在运行时炸，而编译期完全看不出来。
 * 走接口的话，契约是 JSON 字段，改的时候两边都能看见。
 */
export async function reportShipment({ orderId, status, trackingNo, note }) {
  const url = `${config.apiUrl.replace(/\/$/, '')}/api/internal/shipments`;
  const started = performance.now();
  let result = 'ok';

  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Internal-Token': config.internalToken,
      },
      body: JSON.stringify({ orderId, status, trackingNo, note }),
      signal: AbortSignal.timeout(config.callbackTimeoutMs),
    });

    if (res.ok) {
      const body = await res.json().catch(() => ({}));
      // duplicate 表示 Go 侧早就推过了（我们重试导致的），不是错误
      result = body.result === 'duplicate' ? 'duplicate' : 'ok';
      return body;
    }

    const text = await res.text().catch(() => '');
    // 409：状态机不允许。订单可能已经被取消了，这时候再推就是错的。
    // 404：订单不存在。同样不该重试。
    const retryable = res.status >= 500 || res.status === 429;
    result = retryable ? 'retry' : 'fatal';
    throw new CallbackError(
      `回写失败 HTTP ${res.status}: ${text.slice(0, 200)}`,
      { retryable, status: res.status },
    );
  } catch (err) {
    if (err instanceof CallbackError) throw err;
    // fetch 抛出的都是网络层问题（DNS / 连接被拒 / 超时中断）
    result = 'error';
    logger.warn({ orderId, status, err: err.message }, 'callback_network_error');
    throw new CallbackError(`回写网络异常: ${err.message}`, { retryable: true });
  } finally {
    callbackDuration.observe(
      { status, result },
      (performance.now() - started) / 1000,
    );
  }
}
