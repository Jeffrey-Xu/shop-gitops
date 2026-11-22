package store

import (
	"context"
	"time"
)

// OutboxMaxAttempts 是单条消息的尝试上限。到顶之后 relay 不再重试它，
// 由 MarkOutboxDeadExhausted 在超过宽限期后标死（dead_at）。
//
// 为什么要有上限：一条 payload 有结构性问题的消息（topic 拼错、超过
// broker 的 message_max_bytes）重试一万次也不会成功，而 relay 是单线程
// 顺序投递的 —— 它会把后面所有正常消息一起堵住。
const OutboxMaxAttempts = 20

// OutboxDeadAfter 是「尝试用尽」之后还要挂多久才判定为死信。
//
// 光看 attempts 会把瞬时故障误判成毒消息：broker 抖 40 秒，relay 每 2 秒
// 撞一次墙，一批消息的 attempts 就都到顶了。所以再加一条时间条件 ——
// 只有「失败了 20 次」且「这行已经存在 5 分钟以上」才标死。
// 真正的毒消息 5 分钟后照样会被标出来，短暂的 broker 抖动不会被误杀。
const OutboxDeadAfter = 5 * time.Minute

// FetchOutbox 取未投递的消息。
//
// dead_at IS NULL 是必须的：attempts 到顶的消息还留在表里（要留证据），
// 但不能再进投递循环 —— 否则它每轮都失败一次，日志被刷满，
// 而且和「正常消息发不出去」的信号混在一起。
//
// 注意：这里是自动提交的单语句，拿不住行锁；relay 现在是单副本所以没问题。
// 将来 relay 多副本时要改成显式事务 + FOR UPDATE SKIP LOCKED，
// 否则两个 relay 会重复投递（虽然消费端幂等，但没必要浪费）。
func (s *Store) FetchOutbox(ctx context.Context, limit int) ([]OutboxMessage, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT id, topic, payload, attempts, traceparent
		  FROM outbox
		 WHERE published_at IS NULL
		   AND dead_at IS NULL
		   AND attempts < $1
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

// MarkOutboxFailed 记一次失败。last_error 只留最后一次 ——
// 一条消息连续失败 20 次时，有价值的是「最后那次为什么失败」，
// 而不是把 20 条错误串成一段没人读的长文本。
func (s *Store) MarkOutboxFailed(ctx context.Context, id int64, cause string) error {
	_, err := s.DB.ExecContext(ctx,
		`UPDATE outbox SET attempts = attempts + 1, last_error = $2 WHERE id = $1`,
		id, truncateErr(cause))
	return err
}

// MarkOutboxDeadExhausted 把「重试用尽且已经挂够久」的消息标成死信。
//
// 之所以由 relay 每轮扫一遍、而不是在失败当下直接标死：
// 判定条件里有一个时间维度（这行存在多久了），而 relay 只能看到
// 「这一条刚失败」。用一句 UPDATE 让数据库自己判定更准，也省得
// 在 Go 里再写一遍同样的条件。
//
// 返回被标死的条数，relay 拿它打日志。
func (s *Store) MarkOutboxDeadExhausted(ctx context.Context, maxAttempts int, olderThan time.Duration) (int64, error) {
	res, err := s.DB.ExecContext(ctx, `
		UPDATE outbox
		   SET dead_at = now()
		 WHERE published_at IS NULL
		   AND dead_at IS NULL
		   AND attempts >= $1
		   AND created_at < now() - make_interval(secs => $2)`,
		maxAttempts, olderThan.Seconds())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// CountOutboxPending 给告警用：这个数持续上涨说明 relay 或 broker 出问题了。
//
// dead_at IS NULL 与 FetchOutbox 保持一致 —— 三个查询口径必须一样，
// 否则就是「pending 永远降不下来」那个 bug 的翻版：
// 一张表上有三种「未投递」的定义，告警永远说不清自己在报什么。
func (s *Store) CountOutboxPending(ctx context.Context) (int64, error) {
	var n int64
	err := s.DB.QueryRowContext(ctx,
		`SELECT count(*) FROM outbox WHERE published_at IS NULL AND dead_at IS NULL`).Scan(&n)
	return n, err
}

// CountOutboxDead 返回已经放弃投递、等人处理的条数。
//
// 它和 pending 是两个相反的指标：pending 高说明「还在努力」，
// dead 高说明「已经放弃了」。后者为 0 才是正常态，
// 一旦 > 0 就必须有人去 outbox 表里看 last_error。
func (s *Store) CountOutboxDead(ctx context.Context) (int64, error) {
	var n int64
	err := s.DB.QueryRowContext(ctx,
		`SELECT count(*) FROM outbox WHERE published_at IS NULL AND dead_at IS NOT NULL`).Scan(&n)
	return n, err
}

// OldestOutboxAge 返回「最老一条还没投递出去的消息」已经等了多久。
//
// 为什么不只看 pending 计数：broker 半通（能连上、发布超时）时积压涨得很慢，
// 计数看着一切正常，实际最老那条已经躺了几分钟 —— 第一批受影响的订单
// 早就该告警了，而 pending 还是个位数。
// 年龄是唯一能反映「有没有在动」的量：relay 健康时它应该是几秒，
// 一旦开始单调增长，无论计数多少都说明投递链路卡住了。
//
// 排除 dead_at：一条已经被放弃的消息年龄只会一直涨，
// 让它参与计算等于给这条告警装了个永久触发器 —— 那正是修这个 bug 的原因。
func (s *Store) OldestOutboxAge(ctx context.Context) (time.Duration, error) {
	var secs float64
	err := s.DB.QueryRowContext(ctx, `
		SELECT COALESCE(EXTRACT(EPOCH FROM (now() - min(created_at))), 0)::float8
		  FROM outbox
		 WHERE published_at IS NULL AND dead_at IS NULL`).Scan(&secs)
	if err != nil {
		return 0, err
	}
	return time.Duration(secs * float64(time.Second)), nil
}

// truncateErr 保证 last_error 不会因为一条超长的 broker 错误把行撑大。
func truncateErr(s string) string {
	const max = 500
	if len(s) > max {
		return s[:max] + "..."
	}
	return s
}
