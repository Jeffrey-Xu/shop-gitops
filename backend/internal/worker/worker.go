// Package worker 消费订单事件：模拟支付 → 改状态 → 失败则重试 → 超限则补偿。
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/broker"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/cache"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/config"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/obs"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/store"
)

type Worker struct {
	Log    *slog.Logger
	Store  *store.Store
	Cache  *cache.Cache
	Broker *broker.Broker
	Cfg    config.Config
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
	log := w.Log.With("order_id", ev.OrderID, "attempt", attempts+1, "product_id", ev.ProductID)

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

	start := time.Now()
	// 幂等闸门 2：UPDATE ... WHERE status='CREATED' 是乐观锁，
	// 加上 order_events 的 UNIQUE(order_id, to_status)，双保险。
	changed, err := w.Store.MarkOrderPaid(ctx, ev.OrderID, "worker: payment captured (simulated)")
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
		changed, productIDs, err := w.Store.CancelOrder(ctx, ev.OrderID, "worker: payment failed after retries: "+cause.Error())
		if err != nil {
			log.Error("compensation_failed", "err", err)
		} else if changed {
			keys := []string{cache.KeyProductList}
			for _, id := range productIDs {
				keys = append(keys, cache.KeyProduct(id))
			}
			if err := w.Cache.Del(ctx, keys...); err != nil {
				log.Warn("cache_invalidate_failed", "keys", keys, "err", err)
			}
			obs.OrdersProcessed.WithLabelValues("cancelled").Inc()
			log.Error("order_cancelled_after_retries", "err", cause, "max_attempts", w.Cfg.MaxAttempts)
		}

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
