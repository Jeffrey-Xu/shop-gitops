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

	// 以下都是展示元数据，不参与任何金额计算
	Description string `json:"description,omitempty"`
	Category    string `json:"category,omitempty"`
	// Art 是前端插画标识（"keyboard"/"chair"…），不是图片地址。
	// 让前端按同一套矢量风格渲染，也避免把构建产物的路径写进数据库。
	Art    string  `json:"art,omitempty"`
	Rating float64 `json:"rating"`
	Sold   int     `json:"sold"`
	// 划线价：0 表示没有促销，前端不画那条线。
	OriginalPriceCents int64 `json:"originalPriceCents,omitempty"`
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
	// Items 是一单多品之后加的。ProductID/Quantity 仍然保留且取第一行，
	// 因为已经上线的事件消费者可能还在读它们；消息契约只能加不能改。
	Items []OrderItem `json:"items,omitempty"`
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

// OrderLine 是下单请求里的一行。数量在这里校验，价格不接受客户端传入 ——
// 信任客户端报价是被刷单的第一种死法。
type OrderLine struct {
	ProductID int64 `json:"productId"`
	Quantity  int   `json:"quantity"`
}

type CreateOrderParams struct {
	// Items 支持一单多品。旧的单商品调用方可以继续用 ProductID/Quantity，
	// 由 normalize 转成一行 —— 已经发布的前端在滚动更新期间还会打老格式。
	Items          []OrderLine
	ProductID      int64
	Quantity       int
	IdempotencyKey string
	// UserID 为 0 表示匿名下单。匿名是受支持的：不登录也能买，
	// 只是「我的订单」列表里看不到。
	UserID int64
}

// Normalize 把两种请求形态收敛成统一的行列表，并做基本合法性校验。
// 合并相邻的相同商品：一次请求里点两次同一件商品应该是「数量 2」，
// 而不是两行各自扣一次库存（那样合计对得上，但库存校验会各判一次，
// 边界情况下会出现「单行都够、合起来不够」的错误通过）。
func (p CreateOrderParams) Normalize() ([]OrderLine, error) {
	lines := p.Items
	if len(lines) == 0 && p.ProductID > 0 {
		lines = []OrderLine{{ProductID: p.ProductID, Quantity: p.Quantity}}
	}

	merged := make([]OrderLine, 0, len(lines))
	idx := make(map[int64]int, len(lines))
	for _, l := range lines {
		if l.ProductID <= 0 {
			return nil, ErrInvalidProduct
		}
		if l.Quantity <= 0 {
			return nil, ErrInvalidQuantity
		}
		if i, ok := idx[l.ProductID]; ok {
			merged[i].Quantity += l.Quantity
			continue
		}
		idx[l.ProductID] = len(merged)
		merged = append(merged, l)
	}
	if len(merged) == 0 {
		return nil, ErrEmptyCart
	}
	return merged, nil
}

type OutboxMessage struct {
	ID       int64
	Topic    string
	Payload  []byte
	Attempts int
	// Traceparent 是写这行时的 W3C 头。relay 发布时靠它把新 span
	// 挂回下单那一条 trace 上，否则链路在这里断成两截。
	Traceparent string
}
