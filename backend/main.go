package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type Product struct {
	ID    int     `json:"id"`
	Name  string  `json:"name"`
	Price float64 `json:"price"`
	Stock int     `json:"stock"`
}

type Order struct {
	OrderID   int     `json:"orderId"`
	ProductID int     `json:"productId"`
	Amount    float64 `json:"amount"`
}

var (
	db       *sql.DB
	hostname string
)

func main() {
	hostname, _ = os.Hostname()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@postgres:5432/shop?sslmode=disable"
	}

	var err error
	db, err = sql.Open("pgx", dsn)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	go bootstrap()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", handleHealth)
	mux.HandleFunc("/readyz", handleReady)
	mux.HandleFunc("/api/hello", handleHello)
	mux.HandleFunc("/api/products", handleProducts)
	mux.HandleFunc("/api/cart", handleCart)

	addr := ":8080"
	log.Printf("%s listening on %s", hostname, addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

// bootstrap 等数据库就绪后建表并灌种子数据。
// 故意不阻塞启动：DB 晚于 API 起来是常态。
func bootstrap() {
	for i := 1; i <= 30; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := db.PingContext(ctx)
		cancel()
		if err == nil {
			break
		}
		log.Printf("waiting for db (%d/30): %v", i, err)
		time.Sleep(2 * time.Second)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS products (
			id    SERIAL PRIMARY KEY,
			name  TEXT NOT NULL,
			price NUMERIC(10,2) NOT NULL,
			stock INT NOT NULL DEFAULT 0
		);
		CREATE TABLE IF NOT EXISTS products_orders (
			id         SERIAL PRIMARY KEY,
			product_id INT NOT NULL,
			quantity   INT NOT NULL,
			amount     NUMERIC(10,2) NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`)
	if err != nil {
		log.Printf("create table failed: %v", err)
		return
	}

	var count int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM products`).Scan(&count); err != nil {
		log.Printf("count failed: %v", err)
		return
	}
	if count == 0 {
		_, err := db.ExecContext(ctx, `
			INSERT INTO products (name, price, stock) VALUES
			('机械键盘', 599.00, 20),
			('27 寸显示器', 1899.00, 5),
			('人体工学椅', 2399.00, 3)`)
		if err != nil {
			log.Printf("seed failed: %v", err)
			return
		}
		log.Println("seeded 3 products")
	}
	log.Println("bootstrap done")
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "pod": hostname})
}

// readyz 才检查 DB：DB 挂了应该被摘流量，而不是被反复重启。
func handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"ready": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ready": true, "pod": hostname})
}

func handleHello(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"service": "backend",
		"pod":     hostname,
		"path":    r.URL.Path,
		"version": os.Getenv("APP_VERSION"),
	})
}

func handleProducts(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, `SELECT id, name, price, stock FROM products ORDER BY id`)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	defer rows.Close()

	items := []Product{}
	for rows.Next() {
		var p Product
		if err := rows.Scan(&p.ID, &p.Name, &p.Price, &p.Stock); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		items = append(items, p)
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "pod": hostname})
}

// handleCart 演示「事务内扣库存」：条件更新 + 影响行数判断。
func handleCart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "POST only"})
		return
	}

	var req struct {
		ProductID int `json:"productId"`
		Quantity  int `json:"quantity"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "bad json"})
		return
	}
	if req.Quantity <= 0 {
		req.Quantity = 1
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx,
		`UPDATE products SET stock = stock - $1 WHERE id = $2 AND stock >= $1`,
		req.Quantity, req.ProductID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}

	affected, _ := res.RowsAffected()
	if affected == 0 {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error": "库存不足或商品不存在",
			"pod":   hostname,
		})
		return
	}

	var amount float64
	if err := tx.QueryRowContext(ctx,
		`SELECT price * $1 FROM products WHERE id = $2`,
		req.Quantity, req.ProductID).Scan(&amount); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}

	var orderID int
	if err := tx.QueryRowContext(ctx,
		`INSERT INTO products_orders (product_id, quantity, amount) VALUES ($1,$2,$3) RETURNING id`,
		req.ProductID, req.Quantity, amount).Scan(&orderID); err != nil {
		// 订单表不存在时降级成直接返回，不阻塞演示
		log.Printf("insert order failed (ignored): %v", err)
	}

	if err := tx.Commit(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusCreated, Order{
		OrderID:   orderID,
		ProductID: req.ProductID,
		Amount:    amount,
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		fmt.Fprintf(os.Stderr, "encode: %v\n", err)
	}
}
