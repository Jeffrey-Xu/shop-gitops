package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// 对账：把「钱的事实」和「订单的事实」放一起比。
//
// 这两张表由两个服务分别拥有：
//   - payments 归 payment-service（网关账本，钱以它为准）
//   - orders   归 backend（业务状态，发货以它为准）
//
// 在线链路只能保证「正常情况下两者一起前进」：worker 扣款成功后改订单状态，
// 中间任何一步断掉（进程被杀、DB 不可用、网络分区）都会留下一个裂缝。
// 裂缝本身不可怕，可怕的是没人知道它存在 —— 用户不会来投诉「你们多收了钱没发货」，
// 他只会不再下单。
//
// 所以这里不做「预防」（在线链路已经尽力了），只做「发现 + 收敛」：
// 每 15 秒被 api 采样一次导出成指标，需要动手时用 cmd/reconcile。

// MismatchKind 是不一致的类型。每个 kind 对应一种**确定**的处置方式，
// 没有「其它」这一档 —— 归不了类的不一致说明我们对系统的理解是错的，
// 应该让它报错，而不是塞进一个 unknown 里慢慢腐烂。
type MismatchKind string

const (
	// 扣款金额和订单金额不一致。多半是价格在两次读写之间变了，
	// 也可能是有人手工改过库。绝不自动处置：钱数对不上必须人看。
	KindAmountMismatch MismatchKind = "amount_mismatch"

	// 网关已扣款，订单还停在 CREATED。
	// 处置：前推到 PAID（幂等），让履约服务发货。
	KindStuckPaid MismatchKind = "stuck_paid"

	// 网关已扣款，订单却是 CANCELLED —— 钱收了、单作废。
	// 处置：退款。这是唯一「必须动钱」的一类。
	KindPaidButCancelled MismatchKind = "paid_but_cancelled"

	// 订单 PAID，但网关那边是 failed。钱没收到却准备发货。
	// 处置：人工核实（可能是网关侧事后反转，也可能有人改了库）。
	KindPaidButDeclined MismatchKind = "paid_but_declined"

	// 订单停在 CREATED，而网关侧**根本没有这笔扣款记录**。
	//
	// 这一类是最难发现、也最容易被忽略的：outbox 没投出去、消息躺在 DLQ
	// 没人捞、worker 在调网关之前就崩了 —— 结果都是「库存扣了、钱没动、
	// 订单永远不动」。它没有任何一条 payments 行，所以四类里一个都匹配不上。
	// 而它恰恰是对账最该看的东西：用户没被扣钱，但商品卖不出去了。
	//
	// 处置：人工确认（先看 DLQ、再看 backfill），必要时取消订单回收库存。
	// 不自动处置 —— 「没有任何扣款记录」也可能是刚下单还没轮到 worker，
	// 宽限期只能挡住一部分误报，钱没动的情况下误取消比晚发货更糟。
	KindStuckUnpaid MismatchKind = "stuck_unpaid"
)

// ErrPaymentsTableMissing 表示对账的数据源还不存在。
//
// payments 表是 payment-service 建的，本地只跑 api+worker（模拟支付）时
// 根本没有这张表。这不是错误，是「没有可对的东西」——
// 调用方应该跳过，而不是报错让整个采样循环停掉。
var ErrPaymentsTableMissing = errors.New("payments 表不存在：payment-service 还没初始化过库")

// Mismatch 是一行对账差异。
//
// AgeSeconds 用的是 orders.updated_at 而不是 created_at：
// 一个刚被取消的订单，我们想知道「它被取消多久了」，
// 而不是「这张单下了多久」。
type Mismatch struct {
	Kind          MismatchKind
	OrderID       int64
	OrderStatus   string
	PaymentStatus string
	OrderCents    int64
	PaymentCents  int64
	PaymentID     string
	Refunded      bool
	AgeSeconds    float64
}

// 放宽限期之后才算「卡住」。扣款成功到订单落库之间只隔一个事务（毫秒级），
// 所以超过两分钟还是 CREATED，就一定不是「正在处理」。
const DefaultMismatchGrace = 2 * time.Minute

// mismatchPaidSelect 是「网关有扣款记录」的那一支：四类差异都来自
// payments 与 orders 的对比，所以用 INNER JOIN。
const mismatchPaidSelect = `
SELECT CASE
         WHEN p.amount_cents <> o.total_cents THEN 'amount_mismatch'
         WHEN o.status = 'CREATED'   AND p.status = 'succeeded' THEN 'stuck_paid'
         WHEN o.status = 'CANCELLED' AND p.status = 'succeeded' THEN 'paid_but_cancelled'
         ELSE 'paid_but_declined'
       END AS kind,
       o.id,
       o.status,
       p.status,
       o.total_cents,
       p.amount_cents,
       p.payment_id,
       %s AS refunded,
       EXTRACT(EPOCH FROM now() - o.updated_at)::float8 AS age_s
  FROM orders o
  JOIN payments p ON p.order_id = o.id::text
%s
 WHERE o.updated_at < now() - make_interval(secs => $1)
   AND (p.amount_cents <> o.total_cents
     OR (o.status = 'CREATED'   AND p.status = 'succeeded')
     OR (o.status = 'CANCELLED' AND p.status = 'succeeded')
     OR (o.status = 'PAID'      AND p.status = 'failed'))`

// mismatchUnpaidSelect 是「网关没有记录」的那一支，必须是独立的一条查询：
// INNER JOIN 会把这类订单整个过滤掉，这正是它此前完全不可见的原因。
//
// 用 LEFT JOIN + IS NULL 而不是 NOT EXISTS：payments.order_id 是主键，
// anti-join 走得到索引；写成 NOT IN 则会因为 NULL 语义悄悄返回空集。
//
// 只取 CREATED：CANCELLED 且没扣过款是**完全一致**的状态
// （单作废、钱没动），把它算成差异只会制造噪声。
const mismatchUnpaidSelect = `
SELECT 'stuck_unpaid' AS kind,
       o.id,
       o.status,
       '' AS payment_status,
       o.total_cents,
       0::bigint AS amount_cents,
       '' AS payment_id,
       false AS refunded,
       EXTRACT(EPOCH FROM now() - o.updated_at)::float8 AS age_s
  FROM orders o
  LEFT JOIN payments p ON p.order_id = o.id::text
 WHERE p.order_id IS NULL
   AND o.status = 'CREATED'
   AND o.updated_at < now() - make_interval(secs => $1)`

// mismatchSQL 按「refunds 表存不存在」拼两种形态。
//
// 为什么不直接 JOIN refunds：refunds 是随退款功能一起加的，
// 集群里可能先滚 backend 再滚 payment-service，中间那段时间
// 这张表还不存在，JOIN 会让整个对账查询报错。
// 对账工具在「网关是旧版本」时应该照常工作（只是看不到已退款标记）。
func (s *Store) mismatchSQL(ctx context.Context) (detail, counts string, err error) {
	hasPayments, err := s.tableExists(ctx, "payments")
	if err != nil {
		return "", "", err
	}
	if !hasPayments {
		return "", "", ErrPaymentsTableMissing
	}

	hasRefunds, err := s.tableExists(ctx, "refunds")
	if err != nil {
		return "", "", err
	}

	refundedExpr := "false"
	join := ""
	if hasRefunds {
		refundedExpr = "(r.order_id IS NOT NULL)"
		join = "LEFT JOIN refunds r ON r.order_id = p.order_id"
	}

	// 两支 UNION ALL 拼成一个「不一致」集合。两侧列的顺序与类型必须完全一致：
	// 缺 payments 行时 payment_status / payment_id 用空串、金额用 0，
	// 调用方靠 kind 判断该不该读这些字段（stuck_unpaid 一律不读）。
	sel := "(" + fmt.Sprintf(mismatchPaidSelect, refundedExpr, join) + ")"
	sel += " UNION ALL (" + mismatchUnpaidSelect + ")"

	// ORDER BY 只能引用输出列名，不能写 o.id —— UNION 之上已经没有表别名了。
	detail = sel + ` ORDER BY id LIMIT $2`
	// 计数口径和明细不同：明细要留证据（已退款的也列出来，标「已退款」），
	// 计数只算「还需要动手的」。退过款的 paid_but_cancelled 已经收敛 ——
	// 钱回去了、单也作废了，它再继续顶着告警，只会训练人忽略告警。
	counts = `SELECT kind, count(*) FROM (` + sel + `) t
	           WHERE NOT (kind = 'paid_but_cancelled' AND refunded)
	           GROUP BY kind`
	return detail, counts, nil
}

// ScanMismatches 列出所有不一致，最多 limit 条（按订单号升序）。
//
// 顺序固定很重要：分页、重跑、两次结果对比都要可复现。
// 用 ORDER BY id 而不是 ORDER BY age，因为 id 是稳定的、且自带索引。
func (s *Store) ScanMismatches(ctx context.Context, minAge time.Duration, limit int) ([]Mismatch, error) {
	if limit <= 0 {
		limit = 200
	}
	detail, _, err := s.mismatchSQL(ctx)
	if err != nil {
		return nil, err
	}

	rows, err := s.DB.QueryContext(ctx, detail, minAge.Seconds(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]Mismatch, 0, 16)
	for rows.Next() {
		var (
			m    Mismatch
			kind string
		)
		if err := rows.Scan(&kind, &m.OrderID, &m.OrderStatus, &m.PaymentStatus,
			&m.OrderCents, &m.PaymentCents, &m.PaymentID, &m.Refunded, &m.AgeSeconds); err != nil {
			return nil, err
		}
		m.Kind = MismatchKind(kind)
		out = append(out, m)
	}
	return out, rows.Err()
}

// CountMismatches 只返回每类各多少条，给 15s 采样循环用。
//
// 为什么不复用 ScanMismatches 再数长度：卡死 5000 单的时候，
// 把 5000 行读进内存只为了导出一个数字，是拿数据库和 GC 开玩笑。
// COUNT 在库侧做完，返回五行。
//
// 五类都要预置 0：指标是按 kind 分开的 gauge，某一类一条都没有时
// 也必须导出 0，否则「从 0 变成 1」这个最关键的变化在 Prometheus 里
// 表现为「新时间序列出现」，rate/increase 全都算不出来。
func (s *Store) CountMismatches(ctx context.Context, minAge time.Duration) (map[MismatchKind]int64, error) {
	_, counts, err := s.mismatchSQL(ctx)
	if err != nil {
		return nil, err
	}

	rows, err := s.DB.QueryContext(ctx, counts, minAge.Seconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[MismatchKind]int64, 5)
	for _, k := range []MismatchKind{
		KindAmountMismatch, KindStuckPaid, KindStuckUnpaid,
		KindPaidButCancelled, KindPaidButDeclined,
	} {
		out[k] = 0
	}
	for rows.Next() {
		var (
			kind string
			n    int64
		)
		if err := rows.Scan(&kind, &n); err != nil {
			return nil, err
		}
		out[MismatchKind(kind)] = n
	}
	return out, rows.Err()
}

// tableExists 用 to_regclass 而不是查 information_schema：
// 前者一次索引查找，后者要扫系统表；这条查询在 15s 循环里每轮跑两次。
func (s *Store) tableExists(ctx context.Context, name string) (bool, error) {
	var reg sql.NullString
	if err := s.DB.QueryRowContext(ctx,
		`SELECT to_regclass($1)::text`, "public."+name).Scan(&reg); err != nil {
		return false, err
	}
	return reg.Valid, nil
}
