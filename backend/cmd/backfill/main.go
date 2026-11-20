// backfill 给「已付款但没收到 order.paid 事件」的历史订单补一条 outbox 记录。
//
// 为什么需要它：履约服务（fulfillment）上线之前，worker 扣款成功只把订单改成
// PAID，没有写 order.paid 事件。这些单会永久停在 PAID —— 钱收了，货永远不发。
//
// 为什么不直接 UPDATE orders：orders 只允许一个写入口（api 的内部端点 / worker），
// 直接改状态会绕过状态机和 order_events 审计。补 outbox 才是「让它走正常链路」，
// 后面的 relay -> fulfillment -> 回写，每一步都是既有逻辑，不新增特殊路径。
//
// 默认 dry-run，只打印不改数据；加 -apply 才真正写入。
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/config"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/obs"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/store"
)

// 从 order_events 的 note 里把网关支付号捞出来。
// worker 当初写的是 "worker: payment captured (pay_xxx)"，
// 补数据时沿用真号，方便将来对账时能对上。
var paymentIDPattern = regexp.MustCompile(`\((pay_[0-9a-zA-Z]+)\)`)

type candidate struct {
	orderID    int64
	userID     int64
	totalCents int64
	paymentID  string
	items      []store.OrderItem
	paidAt     time.Time
}

func main() {
	apply := flag.Bool("apply", false, "真正写入 outbox（默认只 dry-run 打印）")
	limit := flag.Int("limit", 0, "最多处理多少张订单，0 表示不限")
	dry := flag.Bool("dry-run", false, "显式 dry-run（与不加 -apply 等价）")
	flag.Parse()

	if *dry {
		*apply = false
	}

	cfg, err := config.Load("backfill")
	if err != nil {
		fmt.Fprintln(os.Stderr, "配置错误:", err)
		os.Exit(1)
	}
	log := obs.NewLogger(cfg.LogLevel, "backfill", cfg.Version)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, cfg.DatabaseURL, log)
	if err != nil {
		fmt.Fprintln(os.Stderr, "连数据库失败:", err)
		os.Exit(1)
	}
	defer st.Close()

	list, err := findStuckOrders(ctx, st.DB, *limit)
	if err != nil {
		fmt.Fprintln(os.Stderr, "查询待补订单失败:", err)
		os.Exit(1)
	}

	if len(list) == 0 {
		fmt.Println("没有需要补的订单（所有 PAID 订单都已有 order.paid 事件）。")
		return
	}

	fmt.Printf("找到 %d 张「已付款但无 order.paid 事件」的订单：\n\n", len(list))
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "订单\t用户\t金额(元)\t商品\t支付号\t付款时间")
	for _, c := range list {
		names := make([]string, 0, len(c.items))
		for _, it := range c.items {
			names = append(names, fmt.Sprintf("%s×%d", it.Name, it.Quantity))
		}
		pid := c.paymentID
		if pid == "" {
			pid = "(未记录)"
		}
		uid := "-"
		if c.userID != 0 {
			uid = fmt.Sprint(c.userID)
		}
		fmt.Fprintf(tw, "#%d\t%s\t%.2f\t%s\t%s\t%s\n",
			c.orderID, uid, float64(c.totalCents)/100,
			strings.Join(names, "、"), pid,
			c.paidAt.Local().Format("01-02 15:04"))
	}
	_ = tw.Flush()
	fmt.Println()

	log.Info("backfill_scan", "candidates", len(list), "apply", *apply)

	if !*apply {
		fmt.Println("这是 dry-run，没有改动任何数据。确认无误后加 -apply 再跑一次。")
		return
	}

	n, err := insertOutbox(ctx, st.DB, list)
	if err != nil {
		fmt.Fprintln(os.Stderr, "写入 outbox 失败:", err)
		os.Exit(1)
	}
	log.Info("backfill_applied", "inserted", n)
	fmt.Printf("已写入 %d 条 order.paid 事件。relay 会在下一轮（约 1s）把它们投递到队列，\n", n)
	fmt.Println("fulfillment 收到后自动建发货单，之后按 PENDING -> SHIPPED -> IN_TRANSIT -> DELIVERED 推进。")
	fmt.Println("进度可以用这条语句看：")
	fmt.Println("  SELECT status, count(*) FROM orders WHERE id = ANY('{...}') GROUP BY status;")
}

// findStuckOrders 挑出「PAID + 没有 order.paid 事件 + 还没发过货」的订单。
// 三个条件缺一不可：只按 PAID 筛会把正在履约中的单又补一遍。
func findStuckOrders(ctx context.Context, db *sql.DB, limit int) ([]candidate, error) {
	q := `
		SELECT o.id,
		       COALESCE(o.user_id, 0),
		       o.total_cents,
		       COALESCE((SELECT e.note FROM order_events e
		                  WHERE e.order_id = o.id AND e.to_status = 'PAID'
		                  ORDER BY e.id DESC LIMIT 1), ''),
		       o.updated_at
		  FROM orders o
		 WHERE o.status = 'PAID'
		   AND NOT EXISTS (SELECT 1 FROM outbox b
		                    WHERE b.topic = 'order.paid'
		                      AND (b.payload->>'orderId')::bigint = o.id)
		   AND NOT EXISTS (SELECT 1 FROM order_events e
		                    WHERE e.order_id = o.id AND e.to_status = 'SHIPPED')
		 ORDER BY o.id`
	args := []any{}
	if limit > 0 {
		q += ` LIMIT $1`
		args = append(args, limit)
	}

	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []candidate
	for rows.Next() {
		var (
			c   candidate
			raw string
		)
		if err := rows.Scan(&c.orderID, &c.userID, &c.totalCents, &raw, &c.paidAt); err != nil {
			return nil, err
		}
		if m := paymentIDPattern.FindStringSubmatch(raw); len(m) == 2 {
			c.paymentID = m[1]
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// 商品明细单独查，避免上面的查询因为 json_agg 变复杂。
	for i := range out {
		items, err := loadItems(ctx, db, out[i].orderID)
		if err != nil {
			return nil, err
		}
		out[i].items = items
	}
	return out, nil
}

func loadItems(ctx context.Context, db *sql.DB, orderID int64) ([]store.OrderItem, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT product_id, name, quantity, unit_price_cents
		  FROM order_items WHERE order_id = $1 ORDER BY id`, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]store.OrderItem, 0, 4)
	for rows.Next() {
		var it store.OrderItem
		if err := rows.Scan(&it.ProductID, &it.Name, &it.Quantity, &it.UnitPriceCents); err != nil {
			return nil, err
		}
		items = append(items, it)
	}
	return items, rows.Err()
}

// insertOutbox 在一个事务里逐条插入。
//
// 每条都带 NOT EXISTS 再判一次，因为从扫表到写入之间可能有 relay 已经处理完
// 别的进程补过的单。重复的 order.paid 不会造成重复发货（fulfillment 的
// createShipment 是 ON CONFLICT DO NOTHING），但没必要制造噪音。
func insertOutbox(ctx context.Context, db *sql.DB, list []candidate) (int, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	inserted := 0
	for _, c := range list {
		payload, err := json.Marshal(store.OrderPaidEvent{
			OrderID:    c.orderID,
			UserID:     c.userID,
			TotalCents: c.totalCents,
			PaymentID:  c.paymentID,
			Items:      c.items,
		})
		if err != nil {
			return 0, fmt.Errorf("订单 #%d 事件序列化失败: %w", c.orderID, err)
		}
		res, err := tx.ExecContext(ctx, `
			INSERT INTO outbox (topic, payload)
			SELECT 'order.paid', $1::jsonb
			 WHERE NOT EXISTS (
			       SELECT 1 FROM outbox
			        WHERE topic = 'order.paid'
			          AND (payload->>'orderId')::bigint = $2)`, payload, c.orderID)
		if err != nil {
			return 0, fmt.Errorf("订单 #%d 写 outbox 失败: %w", c.orderID, err)
		}
		n, _ := res.RowsAffected()
		inserted += int(n)
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return inserted, nil
}
