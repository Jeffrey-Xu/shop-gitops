// 链路追踪的初始化。必须在 server.js 的**第一行**被 import：
// Node 的 ESM 会按 import 顺序求值，而 undici / pg 的自动埋点是在
// 模块加载时打补丁的 —— 晚一步加载，那些库已经被 require 过，补丁就打不上了，
// 而且不会报错，只是那条链路永远少一截。
import { NodeSDK } from '@opentelemetry/sdk-node';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { defaultResource, resourceFromAttributes } from '@opentelemetry/resources';
import { UndiciInstrumentation } from '@opentelemetry/instrumentation-undici';
import { PgInstrumentation } from '@opentelemetry/instrumentation-pg';

const endpoint = (process.env.OTEL_EXPORTER_OTLP_ENDPOINT ?? '').replace(/\/+$/, '');

/**
 * 没配 OTEL_EXPORTER_OTLP_ENDPOINT 就不启动 SDK。
 * 本地 node src/server.js 不带 Tempo 也能跑，不会刷一屏导出失败。
 */
export const tracingEnabled = endpoint !== '';

let sdk = null;

if (tracingEnabled) {
  sdk = new NodeSDK({
    // 合并默认资源（SDK 会自己补 telemetry.sdk.*），再盖上我们的标识。
    // 只写自己的字段，别手工拼 sdk 版本 —— 那些由 SDK 负责。
    resource: defaultResource().merge(
      resourceFromAttributes({
        'service.name': process.env.OTEL_SERVICE_NAME ?? 'fulfillment',
        'service.version': process.env.APP_VERSION ?? 'dev',
        'k8s.pod.name': process.env.HOSTNAME ?? '',
        'deployment.environment.name': process.env.APP_ENV ?? 'local',
      }),
    ),
    // exporter 自己会拼 /v1/traces，这里只给根地址
    traceExporter: new OTLPTraceExporter({ url: `${endpoint}/v1/traces` }),
    instrumentations: [
      // Node 18+ 的 fetch 底层是 undici：回写 api 的那一跳靠它出 span，
      // 同时它会把 traceparent 写进请求头，Go 侧才接得上。
      new UndiciInstrumentation(),
      // pg 出 DB span，能看出「订单回写慢」到底是慢在 HTTP 还是 UPDATE
      new PgInstrumentation(),
    ],
  });

  sdk.start();
  // 只打一行。别开 console 的 diag：它会把每条导出都打出来，日志会被淹
  console.log(
    JSON.stringify({
      level: 'info',
      msg: 'tracing_enabled',
      service: process.env.OTEL_SERVICE_NAME ?? 'fulfillment',
      endpoint,
    }),
  );
}

/**
 * 退出前 flush。
 * BatchSpanProcessor 默认攒 5 秒才发一批，进程被 SIGTERM 打断时
 * 缓冲区里那一批就没了 —— 而最关键的信息往往正是崩溃前最后几个 span。
 */
export async function shutdownTracing() {
  if (!sdk) return;
  try {
    await sdk.shutdown();
  } catch (err) {
    console.error(JSON.stringify({ level: 'warn', msg: 'tracing_shutdown_failed', err: err.message }));
  }
}
