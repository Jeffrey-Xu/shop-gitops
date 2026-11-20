import { config } from './config.js';
import { logger } from './log.js';
import { advances, inFlight } from './metrics.js';
import {
  advanceShipment,
  countInFlight,
  dueShipments,
  markFailed,
  rollbackAdvance,
} from './db.js';
import { ORDER_STATUS, TRANSITIONS, stepNote } from './carrier.js';
import { reportShipment } from './api-client.js';

/**
 * 物流推进器。
 *
 * 为什么用「定时扫数据库」而不是把待办放在内存队列里：
 * 进程重启后内存队列就没了，那些 PENDING 的发货单会永远停在原地，
 * 而数据库里的 next_advance_at 不会丢。代价是有一个 tick 周期的延迟，
 * 对物流这种本来就以小时计的流程完全可以接受。
 */

/** 处理一条发货单的下一档。返回推进后的行，没推进则返回 null。 */
export async function advanceOne(shipment) {
  const to = TRANSITIONS[shipment.status];
  if (!to) return null;

  // 先改数据库再回写订单：反过来的话，回写成功但数据库更新失败，
  // 订单状态就跑到发货单前面去了，之后再也不会被扫到（因为状态没变）。
  const row = await advanceShipment(shipment.id, shipment.status, to, {
    stepDelayMs: config.stepDelayMs,
  });
  if (!row) {
    // 另一个副本刚刚抢到这一条，或者状态已经被别人改过
    return null;
  }

  try {
    await reportShipment({
      orderId: Number(row.order_id),
      status: ORDER_STATUS[to],
      trackingNo: row.tracking_no,
      note: stepNote(to),
    });
    advances.inc({ from: shipment.status, to, result: 'ok' });
    logger.info(
      {
        orderId: Number(row.order_id),
        from: shipment.status,
        to,
        trackingNo: row.tracking_no,
      },
      'shipment_advanced',
    );
    return row;
  } catch (err) {
    advances.inc({ from: shipment.status, to, result: 'callback_failed' });

    if (err.retryable) {
      // 可重试：把状态退回去，等下一轮再推。
      // 退回去是安全的 —— Go 侧的回写是幂等的，
      // 即使上次其实成功了，重推也只会返回 duplicate。
      await rollbackAdvance(shipment.id, shipment.status, to, err.message);
      logger.warn(
        { orderId: Number(row.order_id), from: shipment.status, to, err: err.message },
        'advance_rolled_back',
      );
    } else {
      // 不可重试（订单被取消 / 不存在）：停下来等人看，
      // 一小时后才会被重新扫到，给人工处理留出窗口。
      await markFailed(shipment.id, err.message);
      logger.error(
        { orderId: Number(row.order_id), from: shipment.status, to, err: err.message },
        'shipment_failed_permanent',
      );
    }
    return null;
  }
}

/** 扫一轮。批量上限 20：一次处理太多会把单次 tick 拖长，反而降低吞吐。 */
export async function tick() {
  const due = await dueShipments(20);
  if (due.length > 0) {
    logger.debug({ count: due.length }, 'tick_picked');
  }
  for (const s of due) {
    try {
      await advanceOne(s);
    } catch (err) {
      // 单条失败不能拖垮整轮，否则一条坏数据会把所有发货单卡住
      logger.error({ shipmentId: s.id, err: err.message }, 'advance_error');
    }
  }
  try {
    inFlight.set(await countInFlight());
  } catch (err) {
    logger.warn({ err: err.message }, 'in_flight_gauge_failed');
  }
}

/** 启动定时器，返回停止函数。 */
export function startDispatcher() {
  let running = false;
  const timer = setInterval(async () => {
    // 上一轮没跑完就跳过这一轮：慢回调不该让 tick 堆积
    if (running) return;
    running = true;
    try {
      await tick();
    } finally {
      running = false;
    }
  }, config.tickMs);
  // unref 让这个定时器不阻止进程退出
  timer.unref?.();
  logger.info({ tickMs: config.tickMs, stepDelayMs: config.stepDelayMs }, 'dispatcher_started');
  return () => clearInterval(timer);
}
