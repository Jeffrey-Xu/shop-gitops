// Package broker 封装 RabbitMQ：拓扑声明、带 confirm 的发布、消费、积压查询。
//
// 拓扑：
//
//	shop.events (topic)  --order.created-->  shop.order.created
//	shop.dlx    (direct) --order.created.retry--> shop.order.created.retry (per-message TTL，队列级 10s 兜底)
//	                     --order.created.dlq-->   shop.order.created.dlq
//
// 重试不用 nack+requeue：requeue 会立刻重新入队，失败三次也是瞬间完成，
// 等于没有退避。改用「延迟队列」——消息在 retry 队列躺够 TTL 后被死信回主队列。
//
// 延迟由**消息自己**带（PublishDelayed 设 Expiration），不再只靠队列级 TTL。
// 原因：固定延迟会让同一秒失败的一批消息在第 N 秒整批回来，
// 把一次短暂抖动放大成三波尖峰。队列级 TTL 保留为上限兜底。
package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/obs"
)

const (
	Exchange          = "shop.events"
	DLX               = "shop.dlx"
	QueueOrderCreated = "shop.order.created"
	QueueOrderRetry   = "shop.order.created.retry"
	QueueOrderDLQ     = "shop.order.created.dlq"

	RouteOrderCreated = "order.created"
	RouteOrderRetry   = "order.created.retry"
	RouteOrderDLQ     = "order.created.dlq"

	// 支付成功之后的事件，消费方是 Node 写的履约服务。
	// 拓扑仍然由 Go 侧声明（owner 原则）：Node 只 checkQueue + consume，
	// 两边都声明同一个队列且参数不一致会直接 PRECONDITION_FAILED 关连接。
	QueueOrderPaid      = "shop.order.paid"
	QueueOrderPaidRetry = "shop.order.paid.retry"
	QueueOrderPaidDLQ   = "shop.order.paid.dlq"

	RouteOrderPaid      = "order.paid"
	RouteOrderPaidRetry = "order.paid.retry"
	RouteOrderPaidDLQ   = "order.paid.dlq"

	RetryDelayMS = 10000
)

// AllQueues 是全部需要采样的队列，供 api 的 /metrics 采样循环与 replay 工具用。
// 只读，不要就地改。
var AllQueues = []string{
	QueueOrderCreated, QueueOrderRetry, QueueOrderDLQ,
	QueueOrderPaid, QueueOrderPaidRetry, QueueOrderPaidDLQ,
}

type Broker struct {
	conn     *amqp.Connection
	exchange string
	pub      *amqp.Channel
	mu       sync.Mutex
	log      *slog.Logger
}

type Message struct {
	Body    []byte
	Headers amqp.Table
	Ack     func() error
	Nack    func(requeue bool) error
}

func Connect(ctx context.Context, url string, log *slog.Logger) (*Broker, error) {
	var conn *amqp.Connection
	var err error
	for i := 1; i <= 30; i++ {
		conn, err = amqp.Dial(url)
		if err == nil {
			break
		}
		log.Warn("waiting_for_rabbitmq", "attempt", i, "err", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	if err != nil {
		return nil, fmt.Errorf("rabbitmq unreachable: %w", err)
	}

	pub, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	// 开启 publisher confirm：broker 确认落盘后 Wait 才返回，
	// 这样 outbox 里的 published_at 才是可信的。
	if err := pub.Confirm(false); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("enable confirm mode: %w", err)
	}

	b := &Broker{conn: conn, exchange: Exchange, pub: pub, log: log}
	go b.watch()
	return b, nil
}

// watch：连接断了就没有继续跑的必要（重建拓扑、确认未决消息的语义太复杂）。
// 直接退出让 K8s 重启容器，比在进程里假装重连更诚实。
func (b *Broker) watch() {
	err, ok := <-b.conn.NotifyClose(make(chan *amqp.Error, 1))
	if !ok {
		return
	}
	b.log.Error("rabbitmq_connection_lost", "err", err)
	os.Exit(1)
}

func (b *Broker) Close() error {
	if b.pub != nil {
		_ = b.pub.Close()
	}
	return b.conn.Close()
}

func (b *Broker) Declare() error {
	ch, err := b.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	if err := ch.ExchangeDeclare(Exchange, "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare %s: %w", Exchange, err)
	}
	if err := ch.ExchangeDeclare(DLX, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare %s: %w", DLX, err)
	}

	// 主队列：不设 DLX。重试由 worker 显式投递到 dlx，逻辑在代码里，好排查。
	if _, err := ch.QueueDeclare(QueueOrderCreated, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare %s: %w", QueueOrderCreated, err)
	}
	if err := ch.QueueBind(QueueOrderCreated, RouteOrderCreated, Exchange, false, nil); err != nil {
		return err
	}

	// 延迟队列：消息躺满 TTL 后被死信到默认 exchange，
	// routing key 用主队列名字，正好路由回去。
	//
	// x-message-ttl 在这里的角色是**上限兜底**，不是实际延迟：
	// 每条消息自己带 Expiration（见 PublishDelayed），broker 取两者较小值。
	// 保留它是因为「忘了带 Expiration」的消息不至于永久卡在延迟队列里。
	// 改这个值会让已存在的队列参数不一致（PRECONDITION_FAILED），要连带删队列。
	retryArgs := amqp.Table{
		"x-message-ttl":             int32(RetryDelayMS),
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": QueueOrderCreated,
	}
	if _, err := ch.QueueDeclare(QueueOrderRetry, true, false, false, false, retryArgs); err != nil {
		return fmt.Errorf("declare %s: %w", QueueOrderRetry, err)
	}
	if err := ch.QueueBind(QueueOrderRetry, RouteOrderRetry, DLX, false, nil); err != nil {
		return err
	}

	// 死信队列：人工介入用，没有消费者
	if _, err := ch.QueueDeclare(QueueOrderDLQ, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare %s: %w", QueueOrderDLQ, err)
	}
	if err := ch.QueueBind(QueueOrderDLQ, RouteOrderDLQ, DLX, false, nil); err != nil {
		return err
	}

	// ---- order.paid：支付成功 → 履约服务发货 ----
	if _, err := ch.QueueDeclare(QueueOrderPaid, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare %s: %w", QueueOrderPaid, err)
	}
	if err := ch.QueueBind(QueueOrderPaid, RouteOrderPaid, Exchange, false, nil); err != nil {
		return err
	}
	paidRetryArgs := amqp.Table{
		"x-message-ttl":             int32(RetryDelayMS),
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": QueueOrderPaid,
	}
	if _, err := ch.QueueDeclare(QueueOrderPaidRetry, true, false, false, false, paidRetryArgs); err != nil {
		return fmt.Errorf("declare %s: %w", QueueOrderPaidRetry, err)
	}
	if err := ch.QueueBind(QueueOrderPaidRetry, RouteOrderPaidRetry, DLX, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(QueueOrderPaidDLQ, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare %s: %w", QueueOrderPaidDLQ, err)
	}
	if err := ch.QueueBind(QueueOrderPaidDLQ, RouteOrderPaidDLQ, DLX, false, nil); err != nil {
		return err
	}
	return nil
}

func (b *Broker) Publish(ctx context.Context, routingKey string, body []byte, headers amqp.Table) error {
	return b.PublishTo(ctx, b.exchange, routingKey, body, headers)
}

func (b *Broker) PublishTo(ctx context.Context, exchange, routingKey string, body []byte, headers amqp.Table) error {
	return b.publish(ctx, exchange, routingKey, body, headers, 0)
}

// PublishDelayed 投到延迟队列，并让这条消息带自己的 TTL。delay<=0 时退回队列级 TTL。
//
// 为什么需要 per-message TTL：队列级 x-message-ttl 是队列的常量，所有消息一个值，
// 想按「第几次重试」给不同延迟就只能靠消息自带的 Expiration。
//
// 代价必须写下来：RabbitMQ 只在**队头**判定消息是否过期，所以队列里混着长延迟
// 消息时，后面短延迟的会被压到前面那条过期才能出来（head-of-line blocking）。
// 本项目能承受：延迟只有 2s/4s 两档、抖动后最长 6s，偏差上限约一个档位，
// 而抖动的目的本来就是「别同时回来」，这点偏差不破坏目标。
// 如果将来要精确到毫秒、或延迟档位拉开到分钟级，正确做法是
// rabbitmq_delayed_message_exchange 插件（一条消息一个定时器），
// 或者每个档位一个独立延迟队列 —— 都不是在单队列上继续拧 per-message TTL。
func (b *Broker) PublishDelayed(ctx context.Context, exchange, routingKey string, body []byte, headers amqp.Table, delay time.Duration) error {
	if delay > 0 && delay.Milliseconds() >= RetryDelayMS {
		// 队列级 TTL 是 10s，更大的 Expiration 会被 min() 截到 10s，
		// 表面生效、实际不符预期。与其静默截断，不如在日志里留痕。
		b.log.Warn("publish_delay_exceeds_queue_ttl",
			"routing_key", routingKey, "delay_ms", delay.Milliseconds(), "queue_ttl_ms", RetryDelayMS)
	}
	return b.publish(ctx, exchange, routingKey, body, headers, delay)
}

func (b *Broker) publish(ctx context.Context, exchange, routingKey string, body []byte, headers amqp.Table, delay time.Duration) error {
	// MQ 不是 HTTP：没有任何人帮我们传 traceparent。这一跳必须自己注入，
	// 否则链路在第一次跨进程时就断，而且断得无声无息（图上只是少一截）。
	ctx, span := obs.Tracer().Start(ctx, "publish "+routingKey,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "rabbitmq"),
			attribute.String("messaging.operation", "publish"),
			attribute.String("messaging.destination.name", exchange),
			attribute.String("messaging.rabbitmq.routing_key", routingKey),
		))
	defer span.End()

	fail := func(err error) error {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	if headers == nil {
		headers = amqp.Table{}
	}
	otel.GetTextMapPropagator().Inject(ctx, amqpCarrier(headers))

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.pub == nil || b.pub.IsClosed() {
		return fail(errors.New("publish channel closed"))
	}
	dc, err := b.pub.PublishWithDeferredConfirmWithContext(ctx, exchange, routingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		Body:         body,
		Headers:      headers,
		// amqp.Publishing 里 TTL 是**毫秒字符串**（协议字段本来是字符串），
		// 类型写错会被 broker 拒收，不是编译期能拦住的。
		Expiration: expirationFor(delay),
	})
	if err != nil {
		return fail(err)
	}
	ack, err := dc.WaitContext(ctx)
	if err != nil {
		return fail(err)
	}
	if !ack {
		return fail(errors.New("broker nacked the message"))
	}
	return nil
}

// Consume 阻塞消费。prefetch 限制未 ack 的消息数，避免一个 worker 把队列吸干。
func (b *Broker) Consume(ctx context.Context, queue string, prefetch int, handle func(context.Context, Message) error) error {
	ch, err := b.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	if err := ch.Qos(prefetch, 0, false); err != nil {
		return err
	}
	deliveries, err := ch.Consume(queue, "shop-worker", false, false, false, false, nil)
	if err != nil {
		return err
	}
	b.log.Info("consumer_started", "queue", queue, "prefetch", prefetch)

	for {
		select {
		case <-ctx.Done():
			b.log.Info("consumer_stopping", "queue", queue)
			return nil
		case d, ok := <-deliveries:
			if !ok {
				return errors.New("delivery channel closed")
			}
			msg := Message{
				Body:    d.Body,
				Headers: d.Headers,
				Ack:     func() error { return ch.Ack(d.DeliveryTag, false) },
				Nack: func(requeue bool) error {
					return ch.Nack(d.DeliveryTag, false, requeue)
				},
			}
			// 从消息头还原上游 context。基底用进程 ctx 而不是 Background：
			// 收到 SIGTERM 时要能中断正在处理的消息，否则优雅退出会卡住。
			mctx, span := obs.Tracer().Start(
				otel.GetTextMapPropagator().Extract(ctx, amqpCarrier(d.Headers)),
				"consume "+queue,
				trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(
					attribute.String("messaging.system", "rabbitmq"),
					attribute.String("messaging.operation", "process"),
					attribute.String("messaging.destination.name", queue),
				))
			if err := handle(mctx, msg); err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				b.log.Error("handler_failed", "queue", queue, "err", err)
			}
			// 处理完立刻结束。重试是「再投一条新消息」，它会带着新的 traceparent
			// 重新进来，所以每次重试都挂在上一段的下面 —— 一条 trace 里能看全
			// 三次失败加一次成功，而不用按时间戳去猜哪几条日志属于同一次重试。
			span.End()
		}
	}
}

func (b *Broker) QueueDepth(queue string) (int, error) {
	ch, err := b.conn.Channel()
	if err != nil {
		return 0, err
	}
	defer ch.Close()
	q, err := ch.QueueInspect(queue)
	if err != nil {
		return 0, err
	}
	return q.Messages, nil
}

// expirationFor 把 delay 换成 AMQP 的 per-message TTL 字符串。
// 0 表示「不设」，让队列级 TTL 生效 —— 空字符串就是这个意思，
// 写 "0" 反而会让消息立刻过期。
func expirationFor(delay time.Duration) string {
	if delay <= 0 {
		return ""
	}
	ms := delay.Milliseconds()
	if ms < 1 {
		ms = 1
	}
	return strconv.FormatInt(ms, 10)
}

// ReplayOptions 描述一次「把死信放回链路」的搬运。
type ReplayOptions struct {
	Source string // 要读的队列，通常是 DLQ
	// DryRun 时消息原样送回 RestoreRoute（绑在 DLX 上的那条）。
	// 不能用 nack+requeue 代替：requeue 是把消息放回队头，
	// 下一次 Get 还是同一条，循环读会死在这里。
	RestoreRoute   string
	TargetExchange string // 真正重投的目标 exchange
	TargetRoute    string
	Limit          int // 0 用默认上限
	DryRun         bool
	// OnMessage 在每条消息处理前后调用，用于打印。返回 error 表示跳过这一条。
	OnMessage func(headers amqp.Table, body []byte) error
}

// ReplayStats 是搬运结果，用于给运维一个「到底动了多少」的交代。
type ReplayStats struct {
	Matched  int
	Replayed int
	Skipped  int
}

const replayDefaultLimit = 1000

// Replay 把 source 队列里的消息逐条重投回 TargetExchange/TargetRoute。
//
// 两件必须做的事：
//
//  1. **清掉 attempts / last_error**。不清的话，原消息的 attempts 已经是
//     MaxAttempts，消费端一进来就判定超限，立刻再送一次 DLQ —— 看起来
//     「重放成功」，实际上一条都没被处理。
//  2. **保留 traceparent**。重放不是一次新请求，它应该接在原来那条链路上，
//     否则 Tempo 里会出现一棵孤立的树，看不出它重放的是哪一单。
//     具体机制：replay 工具不调用 obs.InitTracing，全局 propagator 保持 noop，
//     PublishTo 里的 Inject 不会写任何东西，消息原有 traceparent 原样带过去。
//     改这个工具时不要顺手加 InitTracing —— 加了就会重新铸一个 trace id。
//
// 单条消息 publish 失败就 nack 回队列并终止：继续往下搬只会把后面的消息
// 也带进不确定状态，不如停下来让人看。
func (b *Broker) Replay(ctx context.Context, opt ReplayOptions) (ReplayStats, error) {
	var st ReplayStats
	limit := opt.Limit
	if limit <= 0 {
		limit = replayDefaultLimit
	}

	ch, err := b.conn.Channel()
	if err != nil {
		return st, err
	}
	defer ch.Close()

	if opt.DryRun {
		// dry-run 把每条消息读出来又投回同一个队列，队列长度始终不变 ——
		// 「读到队列空」这个终止条件永远不成立，会一直读同几条到上限。
		// 所以必须用**开始时的条数**当上限。
		// 副作用：消息被重新入队，顺序会变（内容不变）。
		info, ierr := ch.QueueInspect(opt.Source)
		if ierr != nil {
			// 拿不到深度就不敢往下走：否则要么死循环，要么误删消息。
			return st, fmt.Errorf("inspect %s: %w", opt.Source, ierr)
		}
		if info.Messages < limit {
			limit = info.Messages
		}
	}

	for st.Matched < limit {
		select {
		case <-ctx.Done():
			return st, ctx.Err()
		default:
		}

		d, ok, err := ch.Get(opt.Source, false)
		if err != nil {
			return st, fmt.Errorf("get from %s: %w", opt.Source, err)
		}
		if !ok {
			break // 队列空了
		}
		st.Matched++

		if opt.OnMessage != nil {
			if err := opt.OnMessage(d.Headers, d.Body); err != nil {
				st.Skipped++
				// 跳过的消息必须放回队列，否则等于静默删除
				if nerr := ch.Nack(d.DeliveryTag, false, true); nerr != nil {
					return st, nerr
				}
				continue
			}
		}

		if opt.DryRun {
			// 原样送回 DLQ：不碰 headers、不改 attempts。
			// dry-run 的定义是「什么都不变」，连一条消息的 header 都不该被动过，
			// 否则「先看一眼」就改变了现实，第二次看和第一次不一样。
			if err := b.PublishTo(ctx, DLX, opt.RestoreRoute, d.Body, d.Headers); err != nil {
				if nerr := ch.Nack(d.DeliveryTag, false, true); nerr != nil {
					return st, fmt.Errorf("publish failed (%v) and nack failed: %w", err, nerr)
				}
				return st, err
			}
			if err := ch.Ack(d.DeliveryTag, false); err != nil {
				return st, err
			}
			continue
		}

		headers := d.Headers
		if headers == nil {
			headers = amqp.Table{}
		}
		// 清重试痕迹，但把原始错误挪到一个不参与判定的键上 ——
		// 重放后的消息还能看出「它是从哪次失败里捞回来的」。
		if last, has := headers["last_error"]; has {
			headers["replayed_from_error"] = last
			delete(headers, "last_error")
		}
		headers["replay_count"] = int32(headerInt32(headers, "replay_count") + 1)
		delete(headers, "attempts")

		if err := b.PublishTo(ctx, opt.TargetExchange, opt.TargetRoute, d.Body, headers); err != nil {
			// 投不出去就把原件还回去，别让它停在 unacked 里等连接超时
			if nerr := ch.Nack(d.DeliveryTag, false, true); nerr != nil {
				return st, fmt.Errorf("publish failed (%v) and nack failed: %w", err, nerr)
			}
			return st, err
		}

		if err := ch.Ack(d.DeliveryTag, false); err != nil {
			return st, err
		}
		st.Replayed++
	}
	return st, nil
}

func headerInt32(t amqp.Table, key string) int32 {
	switch v := t[key].(type) {
	case int32:
		return v
	case int64:
		return int32(v)
	default:
		return 0
	}
}

// amqpCarrier 让 amqp.Table 能当 W3C 传播载体用。
//
// 不能直接用 amqp.Table：TextMapCarrier 的接口是 string→string，
// 而表里的值还混着 int32（attempts）和真正的字符串（last_error）。
// Get/Set 只碰字符串，其余原样留着 —— 遍历时也不能把它们当成 trace 头。
type amqpCarrier amqp.Table

func (c amqpCarrier) Get(key string) string {
	if v, ok := c[key].(string); ok {
		return v
	}
	return ""
}

func (c amqpCarrier) Set(key, value string) {
	c[key] = value
}

func (c amqpCarrier) Keys() []string {
	out := make([]string, 0, len(c))
	for k, v := range c {
		if _, ok := v.(string); ok {
			out = append(out, k)
		}
	}
	return out
}
