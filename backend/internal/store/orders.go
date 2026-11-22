package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/obs"
)

// CreateOrder 是整个写路径的核心：
// 扣库存、建订单、记审计、写 outbox —— 全部在同一个事务里。
// 返回的 bool 表示"是否新建"，false 说明命中了幂等键、返回的是已有订单。
//
// 一单多品时逐行扣库存。任何一行库存不足就整单回滚 ——
// 部分成交需要「部分退款 + 部分发货」的模型，那是另一个量级的复杂度，
// 电商的默认语义也是要么全成要么全不成。
func (s *Store) CreateOrder(ctx context.Context, p CreateOrderParams) (Order, bool, error) {
	lines, err := p.Normalize()
	if err != nil {
		return Order{}, false, err
	}

	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return Order{}, false, err
	}
	defer func() { _ = tx.Rollback() }()

	// 按 product_id 升序扣减，让并发下单的加锁顺序一致。
	// 不排序的话，A 单锁 1 再锁 2、B 单锁 2 再锁 1，就是教科书式的死锁。
	// Normalize 已经按首次出现顺序排好了，这里再拍一次 id 序。
	ordered := make([]OrderLine, len(lines))
	copy(ordered, lines)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ProductID < ordered[j].ProductID })

	items := make([]OrderItem, 0, len(ordered))
	var totalCents int64
	for _, l := range ordered {
		var (
			priceCents int64
			name       string
		)
		// 条件更新 + RETURNING：一条语句同时完成「校验库存」和「扣减」，
		// 没有 SELECT 完再 UPDATE 的竞态窗口。
		err = tx.QueryRowContext(ctx, `
			UPDATE products
			   SET stock      = stock - $1,
			       version    = version + 1,
			       updated_at = now()
			 WHERE id = $2 AND stock >= $1
			RETURNING price_cents, name`, l.Quantity, l.ProductID).Scan(&priceCents, &name)
		if errors.Is(err, sql.ErrNoRows) {
			var exists bool
			if e := tx.QueryRowContext(ctx,
				`SELECT EXISTS(SELECT 1 FROM products WHERE id = $1)`, l.ProductID).Scan(&exists); e != nil {
				return Order{}, false, e
			}
			if !exists {
				return Order{}, false, ErrProductNotFound
			}
			return Order{}, false, ErrOutOfStock
		}
		if err != nil {
			return Order{}, false, err
		}

		items = append(items, OrderItem{
			ProductID:      l.ProductID,
			Name:           name,
			Quantity:       l.Quantity,
			UnitPriceCents: priceCents,
		})
		totalCents += priceCents * int64(l.Quantity)
	}

	// 顺带把 created_at / updated_at 取回来：返回值里要给前端，
	// 拿到手就不用提交后再 SELECT 一次。
	var (
		orderID              int64
		createdAt, updatedAt time.Time
	)
	err = tx.QueryRowContext(ctx, `
		INSERT INTO orders (status, total_cents, idempotency_key, user_id)
		VALUES ('CREATED', $1, $2, $3)
		RETURNING id, created_at, updated_at`,
		totalCents, nullIfEmpty(p.IdempotencyKey), nullIfZero(p.UserID)).
		Scan(&orderID, &createdAt, &updatedAt)
	if isUniqueViolation(err) {
		// 幂等键撞了：回滚（库存自动还原），把上一次的结果原样返回
		_ = tx.Rollback()
		existing, e := s.GetOrderByIdempotencyKey(ctx, p.IdempotencyKey)
		if e != nil {
			return Order{}, false, e
		}
		return existing, false, nil
	}
	if err != nil {
		return Order{}, false, err
	}

	for _, it := range items {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO order_items (order_id, product_id, name, quantity, unit_price_cents)
			VALUES ($1, $2, $3, $4, $5)`,
			orderID, it.ProductID, it.Name, it.Quantity, it.UnitPriceCents); err != nil {
			return Order{}, false, err
		}
	}

	// from_status 用空串表示「订单刚创建，还没有前一个状态」
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO order_events (order_id, from_status, to_status, note)
		VALUES ($1, '', 'CREATED', 'api: order created')`, orderID); err != nil {
		return Order{}, false, err
	}

	// ProductID / Quantity 保留第一个商品：worker 只拿它们打日志，
	// 真正扣多少钱看 TotalCents。事件里多带一个 Items 是为了让下游
	// 不必回查订单表也能知道买了什么。
	first := items[0]
	payload, err := json.Marshal(OrderCreatedEvent{
		OrderID:    orderID,
		ProductID:  first.ProductID,
		Quantity:   first.Quantity,
		TotalCents: totalCents,
		Items:      items,
	})
	if err != nil {
		return Order{}, false, err
	}
	// traceparent 一起入库：relay 是定时器驱动的，下单的 span 到那时早就结束了，
	// 只有落库这一个办法能把链路接过去。
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO outbox (topic, payload, traceparent) VALUES ('order.created', $1, $2)`,
		payload, obs.Traceparent(ctx)); err != nil {
		return Order{}, false, err
	}

	if err := tx.Commit(); err != nil {
		return Order{}, false, err
	}

	// 提交后不再回查。
	//
	// 原来这里调 GetOrder，等于刚刚才写进去的东西再从库里读一遍：
	// SELECT orders + SELECT order_items + SELECT order_events，三次往返。
	// 而这三样的内容全在手上 —— id 和两个时间戳是 RETURNING 拿的，
	// 明细就是刚才插入的那个切片，历史事件只有一条（上一段插的那条）。
	//
	// 少这三次往返在本地几乎看不出（亚毫秒），但下单是整条链路的第一跳：
	// 它的耗时会被计入每一次「点一下按钮到看到结果」的体验里，
	// 而这条读路径的收益是零 —— 读回来的值和内存里的值必然相同。
	//
	// 唯一的前提：items 的顺序。这里用 product_id 升序（加锁顺序），
	// GetOrder 用 order_items.id 升序；因为插入就是按 ordered 的顺序，
	// 两者是同一个序列。如果以后改成并发插明细，这个假设就没了。
	order := Order{
		ID:         orderID,
		UserID:     p.UserID,
		Status:     "CREATED",
		TotalCents: totalCents,
		CreatedAt:  createdAt,
		UpdatedAt:  updatedAt,
		Items:      items,
		History: []OrderEvent{{
			FromStatus: "",
			ToStatus:   "CREATED",
			Note:       "api: order created",
			// 同一个事务里的 now() 就是事务开始时间，
			// 所以订单行和这条事件的 created_at 必然相等。
			CreatedAt: createdAt,
		}},
	}
	return order, true, nil
}

func (s *Store) GetOrder(ctx context.Context, id int64) (Order, error) {
	var (
		o          Order
		userID     sql.NullInt64
		trackingNo sql.NullString
	)
	err := s.DB.QueryRowContext(ctx, `
		SELECT id, user_id, status, total_cents, tracking_no, created_at, updated_at
		  FROM orders WHERE id = $1`, id).
		Scan(&o.ID, &userID, &o.Status, &o.TotalCents, &trackingNo, &o.CreatedAt, &o.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Order{}, ErrOrderNotFound
	}
	if err != nil {
		return Order{}, err
	}
	o.UserID = userID.Int64
	o.TrackingNo = trackingNo.String
	if o.Items, err = s.orderItems(ctx, id); err != nil {
		return Order{}, err
	}
	if o.History, err = s.orderHistory(ctx, id); err != nil {
		return Order{}, err
	}
	return o, nil
}

// ListOrdersByUser 返回某个用户的订单，按时间倒序。
//
// 明细和事件用两条 `= ANY($1)` 的批量查询补齐，而不是每单各查一次 ——
// 20 单的话 N+1 就是 41 次往返，在列表页上是能感觉到的。
//
// 刻意不带分页：演示目录的订单量很小，加 offset/limit 要连带把「我的订单」
// 前端改成翻页，收益不成比例。真要做的话游标比 offset 更稳（订单随时在新增，
// offset 翻页会漏单）。
//
// ORDER BY created_at DESC, id DESC 而不是只按 id：orders_user_id_idx 是
// (user_id, created_at DESC)，按 id 排序用不上它，得把该用户的全部订单
// 取出来再排序再截断。id 是自增的、和 created_at 同序，加 id 只为
// 同一毫秒下单时有个确定的次序（分页/对比结果可复现）。
func (s *Store) ListOrdersByUser(ctx context.Context, userID int64, limit int) ([]Order, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT id, user_id, status, total_cents, tracking_no, created_at, updated_at
		  FROM orders
		 WHERE user_id = $1
		 ORDER BY created_at DESC, id DESC
		 LIMIT $2`, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	orders := make([]Order, 0, limit)
	ids := make([]int64, 0, limit)
	for rows.Next() {
		var (
			o          Order
			uid        sql.NullInt64
			trackingNo sql.NullString
		)
		if err := rows.Scan(&o.ID, &uid, &o.Status, &o.TotalCents,
			&trackingNo, &o.CreatedAt, &o.UpdatedAt); err != nil {
			return nil, err
		}
		o.UserID = uid.Int64
		o.TrackingNo = trackingNo.String
		orders = append(orders, o)
		ids = append(ids, o.ID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return orders, nil
	}

	itemsByOrder, err := s.orderItemsBatch(ctx, ids)
	if err != nil {
		return nil, err
	}
	historyByOrder, err := s.orderHistoryBatch(ctx, ids)
	if err != nil {
		return nil, err
	}
	for i := range orders {
		orders[i].Items = itemsByOrder[orders[i].ID]
		orders[i].History = historyByOrder[orders[i].ID]
		if orders[i].Items == nil {
			orders[i].Items = []OrderItem{}
		}
		if orders[i].History == nil {
			orders[i].History = []OrderEvent{}
		}
	}
	return orders, nil
}

func (s *Store) orderItemsBatch(ctx context.Context, ids []int64) (map[int64][]OrderItem, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT order_id, product_id, name, quantity, unit_price_cents
		  FROM order_items WHERE order_id = ANY($1) ORDER BY order_id, id`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[int64][]OrderItem, len(ids))
	for rows.Next() {
		var (
			orderID int64
			it      OrderItem
		)
		if err := rows.Scan(&orderID, &it.ProductID, &it.Name, &it.Quantity, &it.UnitPriceCents); err != nil {
			return nil, err
		}
		out[orderID] = append(out[orderID], it)
	}
	return out, rows.Err()
}

func (s *Store) orderHistoryBatch(ctx context.Context, ids []int64) (map[int64][]OrderEvent, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT order_id, from_status, to_status, note, created_at
		  FROM order_events WHERE order_id = ANY($1) ORDER BY order_id, id`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[int64][]OrderEvent, len(ids))
	for rows.Next() {
		var (
			orderID int64
			e       OrderEvent
		)
		if err := rows.Scan(&orderID, &e.FromStatus, &e.ToStatus, &e.Note, &e.CreatedAt); err != nil {
			return nil, err
		}
		out[orderID] = append(out[orderID], e)
	}
	return out, rows.Err()
}

func (s *Store) GetOrderByIdempotencyKey(ctx context.Context, key string) (Order, error) {
	if key == "" {
		return Order{}, ErrOrderNotFound
	}
	var id int64
	err := s.DB.QueryRowContext(ctx,
		`SELECT id FROM orders WHERE idempotency_key = $1`, key).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return Order{}, ErrOrderNotFound
	}
	if err != nil {
		return Order{}, err
	}
	return s.GetOrder(ctx, id)
}

// GetOrderStatus 是 worker 的幂等闸门：只有 CREATED 才继续处理。
func (s *Store) GetOrderStatus(ctx context.Context, id int64) (string, error) {
	var status string
	err := s.DB.QueryRowContext(ctx, `SELECT status FROM orders WHERE id = $1`, id).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrOrderNotFound
	}
	return status, err
}

// MarkOrderPaid 用 WHERE status='CREATED' 做乐观锁，
// 加上 order_events 的 UNIQUE(order_id, to_status)，重复消费不会把订单改成两次。
//
// 关键在于 order.paid 事件和状态变更在同一个事务里写 outbox：
// 要么「订单变 PAID 且事件待投递」，要么两者都没发生。
// 如果先提交再发消息，中间挂掉就会出现「钱收了但货永远不发」。
func (s *Store) MarkOrderPaid(ctx context.Context, id int64, note, paymentID string) (bool, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()

	res, err := tx.ExecContext(ctx, `
		UPDATE orders SET status = 'PAID', updated_at = now()
		 WHERE id = $1 AND status = 'CREATED'`, id)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return false, nil
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO order_events (order_id, from_status, to_status, note)
		VALUES ($1, 'CREATED', 'PAID', $2)`, id, note); err != nil {
		return false, err
	}

	// 组装履约服务要的完整信息。查不到 items 说明数据坏了，宁可回滚。
	var (
		userID sql.NullInt64
		total  int64
	)
	if err := tx.QueryRowContext(ctx,
		`SELECT user_id, total_cents FROM orders WHERE id = $1`, id).Scan(&userID, &total); err != nil {
		return false, err
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT product_id, name, quantity, unit_price_cents
		  FROM order_items WHERE order_id = $1 ORDER BY id`, id)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	items := make([]OrderItem, 0, 4)
	for rows.Next() {
		var it OrderItem
		if err := rows.Scan(&it.ProductID, &it.Name, &it.Quantity, &it.UnitPriceCents); err != nil {
			return false, err
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return false, err
	}

	payload, err := json.Marshal(OrderPaidEvent{
		OrderID:    id,
		UserID:     userID.Int64,
		TotalCents: total,
		PaymentID:  paymentID,
		Items:      items,
	})
	if err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO outbox (topic, payload, traceparent) VALUES ('order.paid', $1, $2)`,
		payload, obs.Traceparent(ctx)); err != nil {
		return false, err
	}

	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// 状态推进的三种结果。调用方必须区别对待：
//   - ok        状态真的变了
//   - duplicate 已经是目标状态，重复投递，不是错误
//   - illegal   状态机不允许（比如 DELIVERED 直接跳到 PAID），需要人看
var (
	ErrIllegalTransition = errors.New("illegal status transition")
)

// AdvanceShipment 处理履约服务回写的发货/签收。
//
// 幂等的做法和 MarkOrderPaid 一致：条件更新。如果 RowsAffected=0，
// 再查一次当前状态来判断「是重复了」还是「真的不合法」——
// 这两种情况对调用方意味着完全不同的处理（前者 ACK，后者要告警）。
func (s *Store) AdvanceShipment(ctx context.Context, u ShipmentUpdate) (string, error) {
	var from string
	switch u.Status {
	case "SHIPPED":
		from = "PAID"
	case "DELIVERED":
		from = "SHIPPED"
	default:
		return "", ErrIllegalTransition
	}

	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback() }()

	// tracking_no 用 COALESCE(NULLIF(...)) 保留旧值：
	// DELIVERED 的回写不再带运单号，不能把已有的单号冲成 NULL。
	res, err := tx.ExecContext(ctx, `
		UPDATE orders
		   SET status      = $1,
		       tracking_no = COALESCE(NULLIF($2, ''), tracking_no),
		       updated_at  = now()
		 WHERE id = $3 AND status = $4`, u.Status, u.TrackingNo, u.OrderID, from)
	if err != nil {
		return "", err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		var cur string
		err := tx.QueryRowContext(ctx, `SELECT status FROM orders WHERE id = $1`, u.OrderID).Scan(&cur)
		if errors.Is(err, sql.ErrNoRows) {
			return "", ErrOrderNotFound
		}
		if err != nil {
			return "", err
		}
		if cur == u.Status {
			return "duplicate", nil
		}
		return "", ErrIllegalTransition
	}

	note := u.Note
	if note == "" {
		note = "fulfillment: " + u.Status
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO order_events (order_id, from_status, to_status, note)
		VALUES ($1, $2, $3, $4)`, u.OrderID, from, u.Status, note); err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return "ok", nil
}

// CancelOrder 是补偿动作：状态置为 CANCELLED 并把库存加回去。
// 返回受影响的商品 id，调用方拿它去删缓存。
func (s *Store) CancelOrder(ctx context.Context, id int64, note string) (bool, []int64, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, nil, err
	}
	defer func() { _ = tx.Rollback() }()

	res, err := tx.ExecContext(ctx, `
		UPDATE orders SET status = 'CANCELLED', updated_at = now()
		 WHERE id = $1 AND status = 'CREATED'`, id)
	if err != nil {
		return false, nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return false, nil, nil
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE products p
		   SET stock = p.stock + oi.quantity,
		       version = p.version + 1,
		       updated_at = now()
		  FROM order_items oi
		 WHERE oi.order_id = $1 AND p.id = oi.product_id`, id); err != nil {
		return false, nil, err
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO order_events (order_id, from_status, to_status, note)
		VALUES ($1, 'CREATED', 'CANCELLED', $2)`, id, note); err != nil {
		return false, nil, err
	}

	rows, err := tx.QueryContext(ctx,
		`SELECT DISTINCT product_id FROM order_items WHERE order_id = $1`, id)
	if err != nil {
		return false, nil, err
	}
	defer rows.Close()
	var productIDs []int64
	for rows.Next() {
		var pid int64
		if err := rows.Scan(&pid); err != nil {
			return false, nil, err
		}
		productIDs = append(productIDs, pid)
	}
	if err := rows.Err(); err != nil {
		return false, nil, err
	}

	if err := tx.Commit(); err != nil {
		return false, nil, err
	}
	return true, productIDs, nil
}

func (s *Store) OrderStats(ctx context.Context) (map[string]int64, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT status, count(*) FROM orders GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]int64{"CREATED": 0, "PAID": 0, "CANCELLED": 0}
	for rows.Next() {
		var status string
		var n int64
		if err := rows.Scan(&status, &n); err != nil {
			return nil, err
		}
		out[status] = n
	}
	return out, rows.Err()
}

func (s *Store) orderItems(ctx context.Context, orderID int64) ([]OrderItem, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT product_id, name, quantity, unit_price_cents
		  FROM order_items WHERE order_id = $1 ORDER BY id`, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]OrderItem, 0, 4)
	for rows.Next() {
		var it OrderItem
		if err := rows.Scan(&it.ProductID, &it.Name, &it.Quantity, &it.UnitPriceCents); err != nil {
			return nil, err
		}
		items = append(items, it)
	}
	return items, rows.Err()
}

func (s *Store) orderHistory(ctx context.Context, orderID int64) ([]OrderEvent, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT from_status, to_status, note, created_at
		  FROM order_events WHERE order_id = $1 ORDER BY id`, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]OrderEvent, 0, 4)
	for rows.Next() {
		var e OrderEvent
		if err := rows.Scan(&e.FromStatus, &e.ToStatus, &e.Note, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// nullIfZero 把「没登录」的 0 变成 SQL NULL。
// 直接插 0 会撞外键（users 表没有 id=0 的行）。
func nullIfZero(n int64) any {
	if n == 0 {
		return nil
	}
	return n
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
