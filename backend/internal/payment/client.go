// Package payment 是对外部支付网关（payment-service，Python/FastAPI）的最小客户端。
//
// 四个设计要点：
//  1. 拒付（网关正常响应但结果 failed）是**业务终态**，重试一百次也是同一张卡被拒，
//     所以单独用 *DeclinedError 表示，让调用方走「取消订单 + 回补库存」而不是退避重试。
//  2. 超时 / 连接失败 / 5xx / 429 是**临时故障**，返回普通 error，交给 worker 的 retry 链路。
//  3. 幂等由对端保证（order_id 是 payments 的主键），因此重试是安全的：
//     重放只会拿到第一次的结果，不会重复扣款。**但也正因为幂等**，
//     金额校验必须自己查 —— 重放拿到的是第一次的金额，不是这一次请求的。
//  4. 另有两类响应既不是「重试能好」也不是「拒付」，单独建模：
//     *RequestRejectedError（400/422 这类我们自己的 bug）与
//     *AmountMismatchError（钱动了但数目不对）。两者都不许取消订单。
package payment

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/obs"
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

// AmountMismatchError 表示网关返回的金额与我们请求的不一致。
//
// 它几乎总是和幂等一起出现：网关对同一个 order_id 返回**第一次扣款的金额**，
// 所以两次调用之间订单价格一变（改价、优惠券过期、数量被改），
// 就会出现「用户按旧价被扣、订单按新价准备标 PAID」—— 差额没有任何人看得见。
// 只检查 status 是查不出这种事的：status 仍然是 succeeded。
//
// 不重试（重试拿到的是同一笔），也不当拒付（钱已经动了，订单不该被取消）。
type AmountMismatchError struct {
	Want      int64 // 请求的金额（分）
	Got       int64 // 网关记录的金额（分）
	PaymentID string
}

func (e *AmountMismatchError) Error() string {
	return fmt.Sprintf("扣款金额不符: 请求 %d 分，网关 %d 分（payment_id=%s）",
		e.Want, e.Got, e.PaymentID)
}

// RequestRejectedError 表示网关认为请求本身不合法（400 / 404 / 413 / 422…）。
//
// 这类响应几乎一定是调用方自己的 bug（金额为负、字段拼错、请求体超限），
// 重试不会变好；但它和「拒付」是两件事 —— 拒付是卡的问题，这是我们的问题。
// 把它归进 DeclinedError 会让 worker 取消订单，等于替 bug 销毁证据：
// 订单作废、库存回补，代码里的问题一点痕迹都不留。
type RequestRejectedError struct {
	Status int
	Body   string
}

func (e *RequestRejectedError) Error() string {
	return fmt.Sprintf("支付网关拒绝请求（%d）: %s", e.Status, e.Body)
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
		// otelhttp 的 Transport 干两件事：开 client span、把 traceparent 写进请求头。
		// 手写第一步很容易、漏掉第二步也很容易 —— 而漏了第二条，调用照样成功，
		// 只是 trace 在支付网关这一跳断掉，而且要到排障时才会发现。
		hc: &http.Client{
			Timeout: timeout,
			Transport: otelhttp.NewTransport(http.DefaultTransport,
				otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
					return r.Method + " " + r.URL.Path
				}),
			),
		},
		log: log,
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

	switch {
	case resp.StatusCode == http.StatusOK:
		// 继续往下解析
	case resp.StatusCode == http.StatusRequestTimeout,
		resp.StatusCode == http.StatusTooManyRequests,
		resp.StatusCode >= 500:
		// 408 / 429 / 5xx 是明确的「等一下再来」，交给退避重试。
		// 429 尤其重要：把它当拒付会直接把订单取消，而限流只是暂时的 ——
		// 「网关让我慢点」不该变成「这单不卖了」。
		return Result{}, fmt.Errorf("支付网关返回 %d（可重试）: %s", resp.StatusCode, truncate(raw))
	default:
		// 其余 4xx：请求本身不合法，重试无意义；但也不该取消订单。
		// 单独一个类型，让 worker 既不重试也不补偿，只把订单停住等对账。
		return Result{}, &RequestRejectedError{Status: resp.StatusCode, Body: truncate(raw)}
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
		"trace_id", obs.TraceID(ctx),
	)

	if res.Status != "succeeded" {
		return res, &DeclinedError{Reason: res.FailureReason, PaymentID: res.PaymentID}
	}
	// 金额校验放在 status 之后：拒付的响应里金额只是「本来打算扣多少」，
	// 拿它比对没有意义；只有真的扣成功了，金额才是需要核对的事实。
	if res.AmountCents != amountCents {
		return res, &AmountMismatchError{Want: amountCents, Got: res.AmountCents, PaymentID: res.PaymentID}
	}
	return res, nil
}

// RefundResult 是 /refund 的响应体。
type RefundResult struct {
	OrderID     string `json:"order_id"`
	RefundID    string `json:"refund_id"`
	AmountCents int64  `json:"amount_cents"`
	Status      string `json:"status"`
	Reason      string `json:"reason"`
	Replayed    bool   `json:"replayed"`
}

// 退款的两个「不用重试」结局。分开定义是因为调用方要分别处置：
// 前者说明这单根本没扣过款（可能是模拟支付），后者说明扣款本身失败了 ——
// 两种都不该算成「退款失败」，也不该让对账任务反复重试。
var (
	ErrPaymentNotFound = errors.New("payment not found")
	ErrNotRefundable   = errors.New("payment not refundable")
)

// Refund 让网关把某笔已成功的扣款全额退回去。
//
// 幂等由对端保证（refunds.order_id 是主键），所以重试、重放都是安全的 ——
// 第二次调用拿到的是第一次的结果（replayed=true），不会退两笔。
// 注意这里**不**把 4xx 归成 DeclinedError：退款没有「拒付」这个语义，
// 把 404/409 混进去只会让调用方以为「卡被拒了」。
func (c *Client) Refund(ctx context.Context, orderID, reason string) (RefundResult, error) {
	if !c.Enabled() {
		return RefundResult{}, fmt.Errorf("payment client 未配置 PAYMENT_URL")
	}

	body, err := json.Marshal(map[string]any{"order_id": orderID, "reason": reason})
	if err != nil {
		return RefundResult{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/refund", bytes.NewReader(body))
	if err != nil {
		return RefundResult{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if rid := RequestIDFrom(ctx); rid != "" {
		req.Header.Set("X-Request-ID", rid)
	}

	start := time.Now()
	resp, err := c.hc.Do(req)
	if err != nil {
		return RefundResult{}, fmt.Errorf("调用退款接口失败: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return RefundResult{}, fmt.Errorf("读取退款响应失败: %w", err)
	}

	switch resp.StatusCode {
	case http.StatusOK:
		// 继续往下解析
	case http.StatusNotFound:
		return RefundResult{}, ErrPaymentNotFound
	case http.StatusConflict:
		return RefundResult{}, ErrNotRefundable
	default:
		return RefundResult{}, fmt.Errorf("退款接口返回 %d: %s", resp.StatusCode, truncate(raw))
	}

	var res RefundResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return RefundResult{}, fmt.Errorf("解析退款响应失败: %w", err)
	}

	c.log.Info("payment_refunded",
		"order_id", orderID,
		"refund_id", res.RefundID,
		"amount_cents", res.AmountCents,
		"replayed", res.Replayed,
		"dur_ms", time.Since(start).Milliseconds(),
		"trace_id", obs.TraceID(ctx),
	)
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
