// Package broker 封装 RabbitMQ：拓扑声明、带 confirm 的发布、消费、积压查询。
//
// 拓扑：
//
//	shop.events (topic)  --order.created-->  shop.order.created
//	shop.dlx    (direct) --order.created.retry--> shop.order.created.retry (TTL 10s)
//	                     --order.created.dlq-->   shop.order.created.dlq
//
// 重试不用 nack+requeue：requeue 会立刻重新入队，失败三次也是瞬间完成，
// 等于没有退避。改用「延迟队列」——消息在 retry 队列躺 10 秒后被死信回主队列。
package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
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

	RetryDelayMS = 10000
)

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
	return nil
}

func (b *Broker) Publish(ctx context.Context, routingKey string, body []byte, headers amqp.Table) error {
	return b.PublishTo(ctx, b.exchange, routingKey, body, headers)
}

func (b *Broker) PublishTo(ctx context.Context, exchange, routingKey string, body []byte, headers amqp.Table) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.pub == nil || b.pub.IsClosed() {
		return errors.New("publish channel closed")
	}
	dc, err := b.pub.PublishWithDeferredConfirmWithContext(ctx, exchange, routingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		Body:         body,
		Headers:      headers,
	})
	if err != nil {
		return err
	}
	ack, err := dc.WaitContext(ctx)
	if err != nil {
		return err
	}
	if !ack {
		return errors.New("broker nacked the message")
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
			if err := handle(ctx, msg); err != nil {
				b.log.Error("handler_failed", "queue", queue, "err", err)
			}
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
