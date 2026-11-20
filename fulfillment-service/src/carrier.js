// 模拟承运商。真实的对接是「打面单 API 拿运单号 + 订阅轨迹回调」，
// 这里把两头都省了：运单号本地生成，轨迹靠定时器自己推。

export const CARRIERS = [
  { code: 'SF', name: '顺丰速运', prefix: 'SF' },
  { code: 'JD', name: '京东物流', prefix: 'JDV' },
  { code: 'YTO', name: '圆通速递', prefix: 'YT' },
];

// 状态机写成一张表而不是 if/else 链：
// 加一档「派送中」只需要改这里，不用在三个地方找分支。
export const TRANSITIONS = {
  PENDING: 'SHIPPED',
  SHIPPED: 'IN_TRANSIT',
  IN_TRANSIT: 'DELIVERED',
};

// 履约内部状态 → 订单对外状态。
// 内部有四档，客户只关心「发了没」「到了没」，所以 SHIPPED 和 IN_TRANSIT
// 都映射到订单的 SHIPPED。这叫「内部状态比外部状态细」，
// 反过来（外部比内部细）就会逼着内部编造假状态。
export const ORDER_STATUS = {
  SHIPPED: 'SHIPPED',
  IN_TRANSIT: 'SHIPPED',
  DELIVERED: 'DELIVERED',
};

const STEP_NOTES = {
  SHIPPED: 'fulfillment: 已出库，运单已生成',
  IN_TRANSIT: 'fulfillment: 干线运输中',
  DELIVERED: 'fulfillment: 已签收',
};

export function stepNote(status) {
  return STEP_NOTES[status] ?? `fulfillment: ${status}`;
}

/** 按 orderId 稳定选承运商：同一个订单重放时不能换快递公司。 */
export function pickCarrier(orderId) {
  const n = Number(orderId) || 0;
  return CARRIERS[n % CARRIERS.length];
}

/**
 * 运单号也用 orderId 派生，保证幂等：
 * 重复消费时生成的是同一个号，不会出现「一单两个运单」。
 */
export function trackingNumber(carrier, orderId) {
  const n = Math.abs(Number(orderId) || 0);
  const seq = String(n).padStart(10, '0');
  // 末两位做个简单校验位，看起来更像真的
  const check = String((n * 7 + 13) % 100).padStart(2, '0');
  return `${carrier.prefix}${seq}${check}`;
}
