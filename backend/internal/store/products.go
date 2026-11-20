package store

import (
	"context"
	"database/sql"
	"errors"
)

// 商品列表和单品详情共用这一组列。写死在常量里而不是 SELECT *，
// 是为了让「加一列」必须显式改这里 —— 否则 Scan 的目标顺序会悄悄错位。
const productColumns = `id, name, price_cents, stock, description, category, art,
	rating, sold, COALESCE(original_price_cents, 0)`

func scanProduct(row interface{ Scan(...any) error }) (Product, error) {
	var p Product
	err := row.Scan(
		&p.ID, &p.Name, &p.PriceCents, &p.Stock,
		&p.Description, &p.Category, &p.Art,
		&p.Rating, &p.Sold, &p.OriginalPriceCents,
	)
	return p, err
}

func (s *Store) ListProducts(ctx context.Context) ([]Product, error) {
	rows, err := s.DB.QueryContext(ctx,
		`SELECT `+productColumns+` FROM products ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]Product, 0, 16)
	for rows.Next() {
		p, err := scanProduct(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, p)
	}
	return items, rows.Err()
}

func (s *Store) GetProduct(ctx context.Context, id int64) (Product, error) {
	p, err := scanProduct(s.DB.QueryRowContext(ctx,
		`SELECT `+productColumns+` FROM products WHERE id = $1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Product{}, ErrProductNotFound
	}
	return p, err
}
