// Package worker 消费订单事件：扣款 → 改状态 → 失败则重试 → 超限则补偿。
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/broker"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/cache"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/config"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/obs"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/payment"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/store"
)

type Worker struct {
	Log    *slog.Logger
	Store  *store.Store
	Cache  *cache.Cache
	Broker *broker.Broker
	Cfg    config.Config
	// Pay 为 nil 或没配 PAYMENT_URL 时走内置模拟支付
	Pay *payment.Client
}

func (w *Worker) Run(ctx context.Context) error {
	return w.Broker.Consume(ctx, broker.QueueOrderCreated, w.Cfg.Prefetch, w.handle)
}

func (w *Worker) handle(ctx context.Context, msg broker.Message) error {
	var ev store.OrderCreatedEvent
	if err := json.Unmarshal(msg.Body, &ev); err != nil {
		// 解不开的消息重试一万次也没用，直接丢（真实项目应进 DLQ 留证据）
		w.Log.Error("bad_event_payload", "err", err, "body", string(msg.Body))
		_ = msg.Ack()
		return nil
	}

	attempts := headerInt(msg.Headers, "attempts")
	// 把订单号挂到消费 span 上。Tempo 里就能按 order.id 搜到这一单，
	// 不用先知道 trace id 才能查 —— 排障时手上永远只有订单号。
	trace.SpanFromContext(ctx).SetAttributes(
		attribute.Int64("order.id", ev.OrderID),
		attribute.Int("order.attempt", attempts+1),
	)
	// trace_id 进日志：Loki 里点一行就能跳到对应的 trace
	log := w.Log.With("order_id", ev.OrderID, "attempt", attempts+1, "product_id", ev.ProductID, "trace_id", obs.TraceID(ctx))

	// 幂等闸门 1：订单状态。重复投递时这里直接短路。
	status, err := w.Store.GetOrderStatus(ctx, ev.OrderID)
	switch {
	case errors.Is(err, store.ErrOrderNotFound):
		log.Warn("order_not_found_skip")
		_ = msg.Ack()
		return nil
	case err != nil:
		return w.retry(ctx, msg, ev, attempts, log, err)
	}
	if status != "CREATED" {
		log.Info("skip_duplicate_event", "status", status)
		_ = msg.Ack()
		return nil
	}

	// 演示开关：redis-cli SET shop:demo:fail 1 之后所有订单都支付失败，
	// 用来演示重试 3 次 → 补偿（库存回补）→ 进 DLQ。
	// 演示完 redis-cli DEL shop:demo:fail 恢复。
	if w.Cache.FlagSet(ctx, cache.FlagFailNext) {
		log.Warn("payment_failed_injected")
		return w.retry(ctx, msg, ev, attempts, log, errors.New("演示注入：支付渠道超时"))
	}

	// 真正的支付动作。三种结局必须分开，这是这段代码的核心：
	//   成功            → 继续把订单推进到 PAID
	//   拒付 Declined   → 业务终态，重试没意义，直接取消 + 回补库存
	//   其他 error      → 临时故障（超时/5xx），走退避重试
	note, paymentID, err := w.charge(ctx, ev, attempts, log)
	var declined *payment.DeclinedError
	switch {
	case errors.As(err, &declined):
		obs.PaymentCalls.WithLabelValues("declined").Inc()
		w.cancel(ctx, ev, log, "order_cancelled_on_decline",
			"worker: payment declined: "+declined.Reason)
		_ = msg.Ack()
		return nil
	case err != nil:
		obs.PaymentCalls.WithLabelValues("error").Inc()
		return w.retry(ctx, msg, ev, attempts, log, err)
	}

	start := time.Now()
	// 幂等闸门 2：UPDATE ... WHERE status='CREATED' 是乐观锁，
	// 加上 order_events 的 UNIQUE(order_id, to_status)，双保险。
	// order.paid 事件也在同一个事务里写 outbox，履约服务靠它触发发货。
	changed, err := w.Store.MarkOrderPaid(ctx, ev.OrderID, note, paymentID)
	if err != nil {
		return w.retry(ctx, msg, ev, attempts, log, err)
	}
	if !changed {
		log.Info("order_already_finalized")
		_ = msg.Ack()
		return nil
	}

	obs.OrdersProcessed.WithLabelValues("paid").Inc()
	log.Info("order_paid",
		"dur_ms", time.Since(start).Milliseconds(),
		"total_cents", ev.TotalCents,
		"quantity", ev.Quantity,
	)
	_ = msg.Ack()
	return nil
}

// charge 决定走真实网关还是内置模拟，返回要写进 order_events.note 的文案和支付单号。
// paymentID 会随 order.paid 事件传给履约服务，出问题能顺着它去网关那边对账。
func (w *Worker) charge(ctx context.Context, ev store.OrderCreatedEvent, attempts int, log *slog.Logger) (string, string, error) {
	if w.Pay == nil || !w.Pay.Enabled() {
		obs.PaymentCalls.WithLabelValues("disabled").Inc()
		// 模拟支付的单号带 sim_ 前缀，一眼就能和真实网关的区分开
		return "worker: payment captured (simulated)", fmt.Sprintf("sim_%d", ev.OrderID), nil
	}

	// worker 没有入站 HTTP 请求，自己造一个 request id 传给网关；
	// 这样在 Loki 里按同一个值能同时查到 worker 和 payment 两侧的日志。
	rid := fmt.Sprintf("order-%d-try%d", ev.OrderID, attempts+1)
	res, err := w.Pay.Charge(payment.WithRequestID(ctx, rid), strconv.FormatInt(ev.OrderID, 10), ev.TotalCents)
	if err != nil {
		return "", "", err
	}

	obs.PaymentCalls.WithLabelValues("succeeded").Inc()
	log.Info("payment_captured", "payment_id", res.PaymentID, "replayed", res.Replayed)
	return fmt.Sprintf("worker: payment captured (%s)", res.PaymentID), res.PaymentID, nil
}

// retry 决定「再试一次」还是「认输并补偿」。
// 注意先 publish 再 ack：如果 publish 失败就把原消息退回队列，
// 不能既没发出去又把原消息确认掉。
func (w *Worker) retry(ctx context.Context, msg broker.Message, ev store.OrderCreatedEvent, attempts int, log *slog.Logger, cause error) error {
	if attempts+1 >= w.Cfg.MaxAttempts {
		headers := amqp.Table{
			"attempts":   int32(attempts + 1),
			"last_error": cause.Error(),
		}
		if err := w.Broker.PublishTo(ctx, broker.DLX, broker.RouteOrderDLQ, msg.Body, headers); err != nil {
			obs.OrdersProcessed.WithLabelValues("dlq_publish_error").Inc()
			log.Error("dlq_publish_failed", "err", err)
			return msg.Nack(true)
		}

		// 补偿：订单作废 + 库存回补（库存是下单时预扣的）
		w.cancel(ctx, ev, log, "order_cancelled_after_retries",
			"worker: payment failed after retries: "+cause.Error())

		return msg.Ack()
	}

	headers := amqp.Table{
		"attempts":   int32(attempts + 1),
		"last_error": cause.Error(),
	}
	if err := w.Broker.PublishTo(ctx, broker.DLX, broker.RouteOrderRetry, msg.Body, headers); err != nil {
		obs.OrdersProcessed.WithLabelValues("retry_publish_error").Inc()
		log.Error("retry_publish_failed", "err", err)
		return msg.Nack(true)
	}
	obs.OrdersProcessed.WithLabelValues("retry").Inc()
	log.Warn("order_retry_scheduled", "err", cause, "delay_ms", broker.RetryDelayMS)
	return msg.Ack()
}

// cancel 是共用的补偿动作：订单置 CANCELLED + 回补库存 + 删缓存。
// 拒付和重试耗尽两条路径都走它，避免逻辑各写一份然后漂移。
func (w *Worker) cancel(ctx context.Context, ev store.OrderCreatedEvent, log *slog.Logger, logKey, note string) {
	changed, productIDs, err := w.Store.CancelOrder(ctx, ev.OrderID, note)
	if err != nil {
		log.Error("compensation_failed", "err", err)
		return
	}
	if !changed {
		log.Info("order_already_finalized")
		return
	}

	keys := []string{cache.KeyProductList}
	for _, id := range productIDs {
		keys = append(keys, cache.KeyProduct(id))
	}
	if err := w.Cache.Del(ctx, keys...); err != nil {
		log.Warn("cache_invalidate_failed", "keys", keys, "err", err)
	}
	obs.OrdersProcessed.WithLabelValues("cancelled").Inc()
	log.Error(logKey, "reason", note)
}

func headerInt(h amqp.Table, key string) int {
	if h == nil {
		return 0
	}
	switch v := h[key].(type) {
	case int32:
		return int(v)
	case int64:
		return int(v)
	case int:
		return v
	}
	return 0
}
