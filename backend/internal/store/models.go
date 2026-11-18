package store

import "time"

// 金额一律用「分」存整数。
// 原始版本用 float64 + NUMERIC，599.00 这种看着没事，
// 一旦有折扣分摊就会出现 0.30000000000000004。
type Product struct {
	ID         int64  `json:"id"`
	Name       string `json:"name"`
	PriceCents int64  `json:"priceCents"`
	Stock      int    `json:"stock"`
}

type OrderItem struct {
	ProductID      int64  `json:"productId"`
	Name           string `json:"name"`
	Quantity       int    `json:"quantity"`
	UnitPriceCents int64  `json:"unitPriceCents"`
}

type OrderEvent struct {
	FromStatus string    `json:"fromStatus"`
	ToStatus   string    `json:"toStatus"`
	Note       string    `json:"note"`
	CreatedAt  time.Time `json:"createdAt"`
}

type Order struct {
	ID         int64        `json:"id"`
	Status     string       `json:"status"`
	TotalCents int64        `json:"totalCents"`
	CreatedAt  time.Time    `json:"createdAt"`
	UpdatedAt  time.Time    `json:"updatedAt"`
	Items      []OrderItem  `json:"items"`
	History    []OrderEvent `json:"history"`
}

// OrderCreatedEvent 是 outbox 里存的、也是发到 RabbitMQ 的消息体。
// 消息契约一旦上线就只能加字段不能改名，worker 和 api 共用这个结构体
// 就是为了避免两边各写一份然后悄悄漂移。
type OrderCreatedEvent struct {
	OrderID    int64 `json:"orderId"`
	ProductID  int64 `json:"productId"`
	Quantity   int   `json:"quantity"`
	TotalCents int64 `json:"totalCents"`
}

type CreateOrderParams struct {
	ProductID      int64
	Quantity       int
	IdempotencyKey string
}

type OutboxMessage struct {
	ID       int64
	Topic    string
	Payload  []byte
	Attempts int
}
