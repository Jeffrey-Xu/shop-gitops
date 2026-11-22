import pino from 'pino';
import { context, trace } from '@opentelemetry/api';
import { config } from './config.js';

// 输出字段刻意和 Go 侧 slog 对齐：level / msg / service / version。
// Alloy 的 stage.json 只认 "level":"info" 这种字符串，
// pino 默认输出 "level":30（数字），所以必须用 label 覆盖。
export const logger = pino({
  level: config.logLevel,
  base: { service: 'fulfillment', version: config.version },
  timestamp: pino.stdTimeFunctions.isoTime,
  formatters: {
    level: (label) => ({ level: label }),
  },
  // pod 名字放进每条日志，Loki 里能区分副本；
  // trace_id 一起带上，这样在 Loki 里点一行就能跳到 Tempo 里对应的那条链路。
  // 没有活跃 span 时（启动阶段、定时器）不写空字符串 —— 空值会污染按 trace_id 的过滤。
  mixin() {
    const fields = process.env.HOSTNAME ? { pod: process.env.HOSTNAME } : {};
    const sc = trace.getSpan(context.active())?.spanContext();
    if (sc?.traceId) fields.trace_id = sc.traceId;
    return fields;
  },
});

// pino 的方法签名是 log.info(obj, msg)，和 Go 的 log.Info(msg, k, v) 相反。
// 包一层让调用处读起来一致：log('order_paid', { orderId })。
export function logEvent(msg, fields = {}, level = 'info') {
  logger[level](fields, msg);
}
