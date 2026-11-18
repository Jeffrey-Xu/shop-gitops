package store

import (
	"context"
	"database/sql"
	"errors"
)

func (s *Store) ListProducts(ctx context.Context) ([]Product, error) {
	rows, err := s.DB.QueryContext(ctx,
		`SELECT id, name, price_cents, stock FROM products ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]Product, 0, 8)
	for rows.Next() {
		var p Product
		if err := rows.Scan(&p.ID, &p.Name, &p.PriceCents, &p.Stock); err != nil {
			return nil, err
		}
		items = append(items, p)
	}
	return items, rows.Err()
}

func (s *Store) GetProduct(ctx context.Context, id int64) (Product, error) {
	var p Product
	err := s.DB.QueryRowContext(ctx,
		`SELECT id, name, price_cents, stock FROM products WHERE id = $1`, id).
		Scan(&p.ID, &p.Name, &p.PriceCents, &p.Stock)
	if errors.Is(err, sql.ErrNoRows) {
		return Product{}, ErrProductNotFound
	}
	return p, err
}
