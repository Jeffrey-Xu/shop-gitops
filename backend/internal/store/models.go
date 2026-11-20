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
	ID         int64  `json:"id"`
	UserID     int64  `json:"userId,omitempty"`
	Status     string `json:"status"`
	TotalCents int64  `json:"totalCents"`
	// TrackingNo 由履约服务回写，未发货时为空
	TrackingNo string       `json:"trackingNo,omitempty"`
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

// OrderPaidEvent 是支付成功之后发出去的第二个事件，由履约服务（Node）消费。
//
// 为什么把 items 塞进消息而不让履约服务回查 orders 表：
// 跨服务读别人的表 = 数据库变成公共 API，改个列名就要全公司一起上线。
// 消息是契约，表是私有的。
type OrderPaidEvent struct {
	OrderID    int64       `json:"orderId"`
	UserID     int64       `json:"userId,omitempty"`
	TotalCents int64       `json:"totalCents"`
	PaymentID  string      `json:"paymentId,omitempty"`
	Items      []OrderItem `json:"items"`
}

// ShipmentUpdate 是履约服务通过内部接口回写的内容。
// status 只接受 SHIPPED / DELIVERED —— 合法的迁移由 api 侧判定，
// 不把状态机的决定权交给调用方。
type ShipmentUpdate struct {
	OrderID    int64  `json:"orderId"`
	Status     string `json:"status"`
	TrackingNo string `json:"trackingNo"`
	Note       string `json:"note"`
}

type CreateOrderParams struct {
	ProductID      int64
	Quantity       int
	IdempotencyKey string
	// UserID 为 0 表示匿名下单。匿名是受支持的：不登录也能买，
	// 只是「我的订单」列表里看不到。
	UserID int64
}

type OutboxMessage struct {
	ID       int64
	Topic    string
	Payload  []byte
	Attempts int
}
