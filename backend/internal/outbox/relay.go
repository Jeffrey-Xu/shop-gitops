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

		// 先把「重试用尽、且已经挂了足够久」的消息标成死信。
		// 为什么放在每轮开头而不是失败分支里：判定要用「这行存在多久」，
		// 一条消息这轮刚失败、还没到年龄门槛时不该死，下一轮才可能死 ——
		// 交给 SQL 的 created_at 条件比在 Go 里再维护一份状态更准。
		//
		// 没有这一步，attempts 到顶的消息会永远悬空：FetchOutbox 不再取它，
		// 可见性查询又把它算作 pending —— 年龄单调涨、告警永久 firing。
		if n, derr := st.MarkOutboxDeadExhausted(ctx, store.OutboxMaxAttempts, store.OutboxDeadAfter); derr != nil {
			log.Error("outbox_mark_dead_failed", "err", derr)
		} else if n > 0 {
			obs.OutboxPublished.WithLabelValues("dead").Inc()
			log.Error("outbox_dead",
				"count", n,
				"impact", "这些消息不会再投递，订单事件永久丢失，看 outbox.last_error 后用 cmd/backfill 补",
			)
		}

		if len(msgs) == 0 {
			continue
		}

		for _, m := range msgs {
			// 每条消息从库里的 traceparent 起一条支线。
			// 用进程 ctx 直接发的话，publish span 会另起一条 trace，
			// 「下单」和「下单之后发生了什么」就永远对不上了。
			// 套一层超时而不是裸 Background：relay 卡住时要能自己放弃，
			// 不能因为一条消息发不出去就把整个投递循环钉死。
			pctx, cancel := context.WithTimeout(obs.ContextFromTraceparent(m.Traceparent), 10*time.Second)
			err := brk.Publish(pctx, m.Topic, m.Payload, nil)
			cancel()
			if err != nil {
				obs.OutboxPublished.WithLabelValues("error").Inc()
				log.Error("outbox_publish_failed", "outbox_id", m.ID, "topic", m.Topic, "attempts", m.Attempts+1, "err", err)
				if e := st.MarkOutboxFailed(ctx, m.ID, err.Error()); e != nil {
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
