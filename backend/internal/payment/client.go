// Package payment 是对外部支付网关（payment-service，Python/FastAPI）的最小客户端。
//
// 三个设计要点：
//  1. 拒付（网关正常响应但结果 failed）是**业务终态**，重试一百次也是同一张卡被拒，
//     所以单独用 *DeclinedError 表示，让调用方走「取消订单 + 回补库存」而不是退避重试。
//  2. 超时 / 连接失败 / 5xx 是**临时故障**，返回普通 error，交给 worker 的 retry 链路。
//  3. 幂等由对端保证（order_id 是 payments 的主键），因此重试是安全的：
//     重放只会拿到第一次的结果，不会重复扣款。
package payment

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// Result 是网关的响应体。
type Result struct {
	OrderID       string `json:"order_id"`
	Status        string `json:"status"` // succeeded / failed
	PaymentID     string `json:"payment_id"`
	AmountCents   int64  `json:"amount_cents"`
	Currency      string `json:"currency"`
	FailureReason string `json:"failure_reason"`
	Replayed      bool   `json:"replayed"`
}

// DeclinedError 表示「网关明确拒绝」，属于不可重试的业务结果。
type DeclinedError struct {
	Reason    string
	PaymentID string
}

func (e *DeclinedError) Error() string {
	if e.Reason == "" {
		return "支付被拒付"
	}
	return "支付被拒付: " + e.Reason
}

type Client struct {
	baseURL string
	hc      *http.Client
	log     *slog.Logger
}

// New 传入空 baseURL 表示「没接入真实网关」，Enabled() 返回 false，
// worker 会退回内置的模拟支付 —— 这样一套代码在本地和集群都能跑。
func New(baseURL string, timeout time.Duration, log *slog.Logger) *Client {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		hc:      &http.Client{Timeout: timeout},
		log:     log,
	}
}

func (c *Client) Enabled() bool { return c != nil && c.baseURL != "" }

// Charge 对某个订单扣款。amountCents 必须与下单时算出的总额一致，
// 网关侧会把它落成事实，事后对账靠它。
func (c *Client) Charge(ctx context.Context, orderID string, amountCents int64) (Result, error) {
	if !c.Enabled() {
		return Result{}, fmt.Errorf("payment client 未配置 PAYMENT_URL")
	}

	body, err := json.Marshal(map[string]any{
		"order_id":     orderID,
		"amount_cents": amountCents,
	})
	if err != nil {
		return Result{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/charge", bytes.NewReader(body))
	if err != nil {
		return Result{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	// 顺手把追踪用的 request id 传下去：payment 侧会写进它的日志
	if rid := RequestIDFrom(ctx); rid != "" {
		req.Header.Set("X-Request-ID", rid)
	}

	start := time.Now()
	resp, err := c.hc.Do(req)
	if err != nil {
		return Result{}, fmt.Errorf("调用支付网关失败: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// 限制读取长度，避免网关异常时把内存吃满
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return Result{}, fmt.Errorf("读取支付网关响应失败: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		// 4xx 基本是请求本身有问题（金额非法、字段缺失），重试也不会变
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			return Result{}, &DeclinedError{Reason: fmt.Sprintf("gateway_%d: %s", resp.StatusCode, truncate(raw))}
		}
		return Result{}, fmt.Errorf("支付网关返回 %d: %s", resp.StatusCode, truncate(raw))
	}

	var res Result
	if err := json.Unmarshal(raw, &res); err != nil {
		return Result{}, fmt.Errorf("解析支付网关响应失败: %w", err)
	}

	c.log.Info("payment_charged",
		"order_id", orderID,
		"result", res.Status,
		"payment_id", res.PaymentID,
		"amount_cents", res.AmountCents,
		"replayed", res.Replayed,
		"dur_ms", time.Since(start).Milliseconds(),
	)

	if res.Status != "succeeded" {
		return res, &DeclinedError{Reason: res.FailureReason, PaymentID: res.PaymentID}
	}
	return res, nil
}

func truncate(b []byte) string {
	const max = 200
	if len(b) > max {
		return string(b[:max]) + "..."
	}
	return string(b)
}

// ctxKey 是包的私有类型，避免和其他包的同名 key 撞车。
type ctxKey struct{}

// WithRequestID 把请求 id 塞进 context（api 侧由中间件塞过一次，
// worker 侧没有 HTTP 请求，就从消息里取 order_id 拼一个）。
func WithRequestID(ctx context.Context, rid string) context.Context {
	return context.WithValue(ctx, ctxKey{}, rid)
}

func RequestIDFrom(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKey{}).(string); ok {
		return v
	}
	return ""
}
