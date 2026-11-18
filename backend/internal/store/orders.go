package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

// CreateOrder 是整个写路径的核心：
// 扣库存、建订单、记审计、写 outbox —— 全部在同一个事务里。
// 返回的 bool 表示"是否新建"，false 说明命中了幂等键、返回的是已有订单。
func (s *Store) CreateOrder(ctx context.Context, p CreateOrderParams) (Order, bool, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return Order{}, false, err
	}
	defer func() { _ = tx.Rollback() }()

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
		RETURNING price_cents, name`, p.Quantity, p.ProductID).Scan(&priceCents, &name)
	if errors.Is(err, sql.ErrNoRows) {
		var exists bool
		if e := tx.QueryRowContext(ctx,
			`SELECT EXISTS(SELECT 1 FROM products WHERE id = $1)`, p.ProductID).Scan(&exists); e != nil {
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

	totalCents := priceCents * int64(p.Quantity)

	var orderID int64
	err = tx.QueryRowContext(ctx, `
		INSERT INTO orders (status, total_cents, idempotency_key)
		VALUES ('CREATED', $1, $2)
		RETURNING id`, totalCents, nullIfEmpty(p.IdempotencyKey)).Scan(&orderID)
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

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO order_items (order_id, product_id, name, quantity, unit_price_cents)
		VALUES ($1, $2, $3, $4, $5)`,
		orderID, p.ProductID, name, p.Quantity, priceCents); err != nil {
		return Order{}, false, err
	}

	// from_status 用空串表示「订单刚创建，还没有前一个状态」
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO order_events (order_id, from_status, to_status, note)
		VALUES ($1, '', 'CREATED', 'api: order created')`, orderID); err != nil {
		return Order{}, false, err
	}

	payload, err := json.Marshal(OrderCreatedEvent{
		OrderID:    orderID,
		ProductID:  p.ProductID,
		Quantity:   p.Quantity,
		TotalCents: totalCents,
	})
	if err != nil {
		return Order{}, false, err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO outbox (topic, payload) VALUES ('order.created', $1)`, payload); err != nil {
		return Order{}, false, err
	}

	if err := tx.Commit(); err != nil {
		return Order{}, false, err
	}

	order, err := s.GetOrder(ctx, orderID)
	return order, true, err
}

func (s *Store) GetOrder(ctx context.Context, id int64) (Order, error) {
	var o Order
	err := s.DB.QueryRowContext(ctx, `
		SELECT id, status, total_cents, created_at, updated_at
		  FROM orders WHERE id = $1`, id).
		Scan(&o.ID, &o.Status, &o.TotalCents, &o.CreatedAt, &o.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Order{}, ErrOrderNotFound
	}
	if err != nil {
		return Order{}, err
	}
	if o.Items, err = s.orderItems(ctx, id); err != nil {
		return Order{}, err
	}
	if o.History, err = s.orderHistory(ctx, id); err != nil {
		return Order{}, err
	}
	return o, nil
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
func (s *Store) MarkOrderPaid(ctx context.Context, id int64, note string) (bool, error) {
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
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
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

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
