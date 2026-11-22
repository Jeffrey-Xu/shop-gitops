package store

import (
	"context"
	"time"
)

const OutboxMaxAttempts = 20

// FetchOutbox 取未投递的消息。
// 注意：这里是自动提交的单语句，拿不住行锁；relay 现在是单副本所以没问题。
// 将来 relay 多副本时要改成显式事务 + FOR UPDATE SKIP LOCKED，
// 否则两个 relay 会重复投递（虽然消费端幂等，但没必要浪费）。
func (s *Store) FetchOutbox(ctx context.Context, limit int) ([]OutboxMessage, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT id, topic, payload, attempts, traceparent
		  FROM outbox
		 WHERE published_at IS NULL AND attempts < $1
		 ORDER BY id
		 LIMIT $2`, OutboxMaxAttempts, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]OutboxMessage, 0, limit)
	for rows.Next() {
		var m OutboxMessage
		if err := rows.Scan(&m.ID, &m.Topic, &m.Payload, &m.Attempts, &m.Traceparent); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *Store) MarkOutboxPublished(ctx context.Context, id int64) error {
	_, err := s.DB.ExecContext(ctx,
		`UPDATE outbox SET published_at = now() WHERE id = $1`, id)
	return err
}

func (s *Store) MarkOutboxFailed(ctx context.Context, id int64) error {
	_, err := s.DB.ExecContext(ctx,
		`UPDATE outbox SET attempts = attempts + 1 WHERE id = $1`, id)
	return err
}

// CountOutboxPending 给告警用：这个数持续上涨说明 relay 或 broker 出问题了。
func (s *Store) CountOutboxPending(ctx context.Context) (int64, error) {
	var n int64
	err := s.DB.QueryRowContext(ctx,
		`SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&n)
	return n, err
}

// OldestOutboxAge 返回「最老一条还没投递出去的消息」已经等了多久。
//
// 为什么不只看 pending 计数：broker 半通（能连上、发布超时）时积压涨得很慢，
// 计数看着一切正常，实际最老那条已经躺了几分钟 —— 第一批受影响的订单
// 早就该告警了，而 pending 还是个位数。
// 年龄是唯一能反映「有没有在动」的量：relay 健康时它应该是几秒，
// 一旦开始单调增长，无论计数多少都说明投递链路卡住了。
func (s *Store) OldestOutboxAge(ctx context.Context) (time.Duration, error) {
	var secs float64
	err := s.DB.QueryRowContext(ctx, `
		SELECT COALESCE(EXTRACT(EPOCH FROM (now() - min(created_at))), 0)::float8
		  FROM outbox
		 WHERE published_at IS NULL`).Scan(&secs)
	if err != nil {
		return 0, err
	}
	return time.Duration(secs * float64(time.Second)), nil
}
