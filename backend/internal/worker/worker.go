// Package worker 消费订单事件：扣款 → 改状态 → 失败则重试 → 超限则补偿。
//
// 补偿分两种，取决于钱有没有动：
//   - 还没扣到钱（超时、5xx）：取消订单 + 回补库存，天经地义。
//   - 已经扣到钱、只是状态没落库：绝不取消。订单留在 CREATED，
//     payment_id 跟着消息进 DLQ，由 reconcile 前推或 replay 重放。
//     路径搞反了就是「钱收了、货不发、单还作废」。
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"strings"
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
		// 解不开的消息重试一万次也没用，但不能悄悄 ack 掉 ——
		// 直接丢等于把「有一单没被处理」这个事实一起删了。
		// 原样扔进 DLQ，至少留下证据和 body。
		w.Log.Error("bad_event_payload", "err", err, "body", string(msg.Body))
		headers := amqp.Table{"last_error": "unmarshal: " + err.Error()}
		if perr := w.Broker.PublishTo(ctx, broker.DLX, broker.RouteOrderDLQ, msg.Body, headers); perr != nil {
			w.Log.Error("bad_payload_dlq_failed", "err", perr)
			return msg.Nack(true)
		}
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

	// 上一次尝试已经扣款成功、只是订单状态没落库（或者这条消息被 replay 回来）。
	// 这时绝不能再调一次网关：钱已经扣了，要做的只是把订单推进到 PAID。
	//
	// payment_id 随消息头传下来，而 replay 只清 attempts、保留其它头，
	// 所以「把 DLQ 里那条消息重投一遍」就能救回「扣了款没改状态」的单 ——
	// 不需要人工去网关查单号再手改数据库。
	paymentID := headerString(msg.Headers, "payment_id")
	note := ""
	if paymentID != "" {
		obs.PaymentCalls.WithLabelValues("already_charged").Inc()
		note = chargeNote(paymentID)
		log.Info("reuse_captured_payment", "payment_id", paymentID)
	} else {
		// 真正的支付动作。四种结局必须分开，这是这段代码的核心：
		//   成功            → 继续把订单推进到 PAID
		//   拒付 Declined   → 业务终态，重试没意义，直接取消 + 回补库存
		//   金额不符        → 钱动了但不能记账，进 DLQ 等人核实（绝不取消）
		//   请求被拒(4xx)   → 我们自己的 bug，进 DLQ 但不取消（走了就看不见了）
		//   其他 error      → 临时故障（超时/5xx/429），走退避重试
		var cerr error
		note, paymentID, cerr = w.charge(ctx, ev, attempts, log)
		var (
			declined *payment.DeclinedError
			mismatch *payment.AmountMismatchError
			rejected *payment.RequestRejectedError
		)
		switch {
		case errors.As(cerr, &declined):
			// 补偿必须做成之后再 ack。做不成（DB 抖动）就退回队列重来 ——
			// 先 ack 再补偿的话，补偿失败就只剩一行日志：
			// 订单停在 CREATED、库存没回补、消息已经消失，三样全错。
			obs.PaymentCalls.WithLabelValues("declined").Inc()
			if err := w.cancel(ctx, ev, log, "order_cancelled_on_decline",
				"worker: payment declined: "+declined.Reason); err != nil {
				return msg.Nack(true)
			}
			_ = msg.Ack()
			return nil

		case errors.As(cerr, &mismatch):
			// 网关那边已经落账（只是金额和我们以为的不一样），
			// 所以这条路径和「扣款后收尾失败」同级：不许取消订单。
			// 进 DLQ 留证据，对账会把它归到 amount_mismatch，等人核实。
			obs.PaymentCalls.WithLabelValues("amount_mismatch").Inc()
			return w.halt(ctx, msg, log, attempts,
				"支付金额不符: "+mismatch.Error(), mismatch.PaymentID)

		case errors.As(cerr, &rejected):
			// 400/422 说明请求本身有问题（金额为负、字段缺失），是我们的 bug。
			// 把它当拒付去取消订单，等于拿用户当 bug 的替罪羊：
			// 订单作废、库存回补，而代码里的问题一点痕迹都不留。
			// 停在这里，让对账的 stuck_unpaid 每天把它顶出来。
			obs.PaymentCalls.WithLabelValues("rejected").Inc()
			return w.halt(ctx, msg, log, attempts, rejected.Error(), "")

		case cerr != nil:
			obs.PaymentCalls.WithLabelValues("error").Inc()
			return w.retry(ctx, msg, ev, attempts, log, cerr)
		}
	}

	start := time.Now()
	// 幂等闸门 2：UPDATE ... WHERE status='CREATED' 是乐观锁，
	// 加上 order_events 的 UNIQUE(order_id, to_status)，双保险。
	// order.paid 事件也在同一个事务里写 outbox，履约服务靠它触发发货。
	changed, err := w.Store.MarkOrderPaid(ctx, ev.OrderID, note, paymentID)
	if err != nil {
		// 走到这里钱已经在网关侧落了账。这条路径上**不允许** cancel ——
		// 取消会把订单置成 CANCELLED 并回补库存，而钱已经扣了：
		// 「货不发、钱不退、库存在」是最糟的一种不一致。
		// 正确做法是把 payment_id 带进重试/死信，让后续 attempt 或
		// 对账任务把订单前推到 PAID。
		return w.retryAfterCharge(ctx, msg, ev, attempts, log, err, paymentID)
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
		pid := fmt.Sprintf("sim_%d", ev.OrderID)
		return chargeNote(pid), pid, nil
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
	return chargeNote(res.PaymentID), res.PaymentID, nil
}

// chargeNote 只由 paymentID 决定，必须是纯函数。
// 因为重试时消息头里只有 payment_id，note 是照着它重新拼出来的 ——
// 两处格式一旦漂移，order_events 里同一个单会出现两种文案。
func chargeNote(paymentID string) string {
	if strings.HasPrefix(paymentID, "sim_") {
		return "worker: payment captured (simulated)"
	}
	return fmt.Sprintf("worker: payment captured (%s)", paymentID)
}

// retry 决定「再试一次」还是「认输并补偿」。
// 只用于**还没扣到钱**的失败：超时、5xx、连接失败都属于这一类，
// 认输时取消订单 + 回补库存是完全正确的补偿。
func (w *Worker) retry(ctx context.Context, msg broker.Message, ev store.OrderCreatedEvent, attempts int, log *slog.Logger, cause error) error {
	return w.reschedule(ctx, msg, ev, attempts, log, cause, "")
}

// retryAfterCharge 处理「钱已经扣了，但订单状态没落库」的失败（几乎都是 DB 侧的）。
//
// 和 retry 的唯一差别：认输时不 cancel，也不回补库存 —— 那条路会让
// 「已扣款」变成「订单作废且不退款」。这里把 payment_id 写进消息头之后
// 再进 DLQ：下一次尝试（或一周后有人跑 replay）读到它就直接前推到 PAID，
// 一次网关调用都不用发。
//
// 如果连 DLQ 都发不出去，就 nack(requeue) 把原消息退回主队列，
// 至少保证「钱扣了」这件事不会随着消息一起消失。
func (w *Worker) retryAfterCharge(ctx context.Context, msg broker.Message, ev store.OrderCreatedEvent, attempts int, log *slog.Logger, cause error, paymentID string) error {
	return w.reschedule(ctx, msg, ev, attempts, log, cause, paymentID)
}

// reschedule 是两条重试路径的公共实现。paymentID 非空表示「这一单已经扣过款」，
// 它决定两件事：消息头里多带 payment_id，以及耗尽重试时不做取消补偿。
//
// 顺序上先 publish 再 ack：如果 publish 失败就把原消息退回队列，
// 不能既没发出去又把原消息确认掉 —— 那等于消息凭空消失。
func (w *Worker) reschedule(ctx context.Context, msg broker.Message, ev store.OrderCreatedEvent, attempts int, log *slog.Logger, cause error, paymentID string) error {
	headers := amqp.Table{
		"attempts":   int32(attempts + 1),
		"last_error": cause.Error(),
	}
	if paymentID != "" {
		headers["payment_id"] = paymentID
	}

	if attempts+1 >= w.Cfg.MaxAttempts {
		if paymentID != "" {
			// 订单故意留在 CREATED：钱已经收了，这个单必须被前推，
			// 不能被补偿成 CANCELLED。reconcile 会把它算进 stuck_paid，
			// replay 这条 DLQ 消息也能带上 payment_id 直接重放。
			if err := w.Broker.PublishTo(ctx, broker.DLX, broker.RouteOrderDLQ, msg.Body, headers); err != nil {
				obs.OrdersProcessed.WithLabelValues("dlq_publish_error").Inc()
				log.Error("dlq_publish_failed", "err", err)
				return msg.Nack(true)
			}
			obs.OrdersProcessed.WithLabelValues("dlq_after_charge").Inc()
			log.Error("finalize_failed_after_charge",
				"payment_id", paymentID,
				"impact", "订单停在 CREATED，钱已扣；用 reconcile 前推或 replay 重放",
				"err", cause)
			return msg.Ack()
		}

		// 还没扣到钱：先补偿、后留证据。顺序不能反 ——
		// 先发 DLQ 再取消的话，取消一失败就变成「消息躺在 DLQ 等人捞，
		// 而订单还活着、库存还扣着」，DLQ 里的证据反而把人引向错误的方向。
		// 反过来「取消成功、DLQ 发布失败」是可以接受的：Nack(true) 让消息
		// 重新投递，消费端看到 CANCELLED 会 skip，代价只是少一条死信记录。
		if err := w.cancel(ctx, ev, log, "order_cancelled_after_retries",
			"worker: payment failed after retries: "+cause.Error()); err != nil {
			return msg.Nack(true)
		}

		if err := w.Broker.PublishTo(ctx, broker.DLX, broker.RouteOrderDLQ, msg.Body, headers); err != nil {
			obs.OrdersProcessed.WithLabelValues("dlq_publish_error").Inc()
			log.Error("dlq_publish_failed", "err", err)
			return msg.Nack(true)
		}
		return msg.Ack()
	}

	delay := w.retryDelay(attempts)
	if err := w.Broker.PublishDelayed(ctx, broker.DLX, broker.RouteOrderRetry, msg.Body, headers, delay); err != nil {
		obs.OrdersProcessed.WithLabelValues("retry_publish_error").Inc()
		log.Error("retry_publish_failed", "err", err)
		return msg.Nack(true)
	}
	obs.OrdersProcessed.WithLabelValues("retry").Inc()
	log.Warn("order_retry_scheduled", "err", cause, "delay_ms", delay.Milliseconds(), "charged", paymentID != "")
	return msg.Ack()
}

// retryDelay 算这次重试该等多久：base * 2^attempts，再乘 [0.5,1.5) 的随机系数。
//
// 两个都必须有，缺哪个都不行：
//   - 指数：让重试给上游留出恢复时间。固定 10 秒时，一个需要 30 秒才能好的
//     依赖会被打三次，每次都撞在它还病着的时候。
//   - 抖动：同一秒失败的一批订单，如果延迟完全相同，就会在第 N 秒、第 2N 秒
//     整批回来。那不是「重试」，是把一次瞬时抖动复制成三波尖峰 ——
//     下游刚缓过来就被同一批请求再打一次。
//
// 上限取 broker.RetryDelayMS（队列级 TTL）：per-message Expiration 大于它会被
// broker 截断，表面生效实际不符。所以 base 与 MaxAttempts 要一起看：
// 现在 base=2s、最多重试 2 次 → 2s / 4s，乘 1.5 后最长 6s，仍在 10s 以内。
func (w *Worker) retryDelay(attempts int) time.Duration {
	base := w.Cfg.RetryBaseDelay
	if base <= 0 {
		base = 2 * time.Second
	}
	d := base << attempts // attempts 已被调用方限定在 [0, MaxAttempts)
	limit := time.Duration(broker.RetryDelayMS) * time.Millisecond
	if d > limit {
		d = limit
	}
	d = time.Duration(float64(d) * (0.5 + rand.Float64()))
	if d > limit {
		d = limit
	}
	if d < time.Millisecond {
		d = time.Millisecond
	}
	return d
}

// halt 把消息直接放进 DLQ 并 ack：语义是「不要再自动试了」。
//
// 和 retryAfterCharge 的区别在于它不是「暂时失败」，而是「继续下去会
// 制造更糟的状态」：金额不符时把订单标 PAID 等于按错价发货；
// 请求被拒时取消订单等于替 bug 销毁证据。这两种都该停在原地等人看。
//
// 注意 ack 只在 publish 成功之后：发不出去就退回队列，
// 「钱动了但记录没了」是这里最不能出现的结果。
func (w *Worker) halt(ctx context.Context, msg broker.Message, log *slog.Logger, attempts int, reason, paymentID string) error {
	headers := amqp.Table{
		// 直接把 attempts 顶到上限：replay 工具靠它判断这条消息还值不值得重投
		"attempts":   int32(w.Cfg.MaxAttempts),
		"last_error": reason,
		"halted":     true,
	}
	if paymentID != "" {
		headers["payment_id"] = paymentID
	}
	if err := w.Broker.PublishTo(ctx, broker.DLX, broker.RouteOrderDLQ, msg.Body, headers); err != nil {
		obs.OrdersProcessed.WithLabelValues("dlq_publish_error").Inc()
		log.Error("halt_publish_failed", "err", err)
		return msg.Nack(true)
	}
	obs.OrdersProcessed.WithLabelValues("halted").Inc()
	log.Error("order_halted",
		"reason", reason,
		"payment_id", paymentID,
		"attempts", attempts+1,
		"impact", "订单状态未变，等人工核实；reconcile 会把它列出来",
	)
	return msg.Ack()
}

// cancel 是共用的补偿动作：订单置 CANCELLED + 回补库存 + 删缓存。
// 拒付和重试耗尽两条路径都走它，避免逻辑各写一份然后漂移。
//
// 返回 error 是刻意的：调用方必须先确认补偿真的做成了才能 ack。
// 之前这里失败只打一行日志，而调用方无条件 ack —— DB 抖一下就变成
// 「订单停在 CREATED、库存没回补、消息已经消失」，而且对账看不见
// （没有 payments 行）。补偿失败是可重试的，把消息退回去即可。
func (w *Worker) cancel(ctx context.Context, ev store.OrderCreatedEvent, log *slog.Logger, logKey, note string) error {
	changed, productIDs, err := w.Store.CancelOrder(ctx, ev.OrderID, note)
	if err != nil {
		log.Error("compensation_failed", "err", err,
			"impact", "订单未作废、库存未回补；消息会退回队列重试")
		return err
	}
	if !changed {
		log.Info("order_already_finalized")
		return nil
	}

	keys := []string{cache.KeyProductList}
	for _, id := range productIDs {
		keys = append(keys, cache.KeyProduct(id))
	}
	// 缓存失效失败不影响正确性：key 有 30s TTL，最坏情况是 30 秒内
	// 商品页显示一个已经被买走的库存数。为此把已经成功的补偿退回去
	// （Nack 重跑）反而会重复回补库存，更糟。
	if err := w.Cache.Del(ctx, keys...); err != nil {
		log.Warn("cache_invalidate_failed", "keys", keys, "err", err)
	}
	obs.OrdersProcessed.WithLabelValues("cancelled").Inc()
	log.Error(logKey, "reason", note)
	return nil
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

// headerString 取字符串头。JSON 往返、别的客户端投递时类型可能是
// string 也可能是 []byte，两种都认，否则 payment_id 会静默读成空串 ——
// 那样重试就会再去扣一次款（幂等兜住了，但白跑一趟网关，日志也会骗人）。
func headerString(h amqp.Table, key string) string {
	if h == nil {
		return ""
	}
	switch v := h[key].(type) {
	case string:
		return v
	case []byte:
		return string(v)
	}
	return ""
}
