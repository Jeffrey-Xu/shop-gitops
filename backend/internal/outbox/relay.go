// Package outbox 实现事务性 outbox 的投递端（relay）。
//
// api 在订单事务里写一行 outbox；这里轮询未投递的行，publish 成功后才标记
// published_at。进程在「publish 成功」和「标记成功」之间挂掉，消息会重发一次
// —— 所以消费端必须幂等，这是 at-least-once 的代价，也是它的定义。
package outbox

import (
	"context"
	"log/slog"
	"time"

	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/broker"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/obs"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/store"
)

func Run(ctx context.Context, log *slog.Logger, st *store.Store, brk *broker.Broker, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("outbox_relay_stopped")
			return
		case <-ticker.C:
		}

		msgs, err := st.FetchOutbox(ctx, 50)
		if err != nil {
			log.Error("outbox_fetch_failed", "err", err)
			continue
		}
		if len(msgs) == 0 {
			continue
		}

		for _, m := range msgs {
			if err := brk.Publish(ctx, m.Topic, m.Payload, nil); err != nil {
				obs.OutboxPublished.WithLabelValues("error").Inc()
				log.Error("outbox_publish_failed", "outbox_id", m.ID, "topic", m.Topic, "attempts", m.Attempts+1, "err", err)
				if e := st.MarkOutboxFailed(ctx, m.ID); e != nil {
					log.Error("outbox_mark_failed", "outbox_id", m.ID, "err", e)
				}
				// broker 有问题时后面的也发不出去，等下一轮
				break
			}
			if err := st.MarkOutboxPublished(ctx, m.ID); err != nil {
				log.Error("outbox_mark_published_failed", "outbox_id", m.ID, "err", err)
				continue
			}
			obs.OutboxPublished.WithLabelValues("ok").Inc()
			log.Info("outbox_published", "outbox_id", m.ID, "topic", m.Topic)
		}
	}
}
