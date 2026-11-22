package payment

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Charge 的错误分类是整个 worker 补偿方向的输入：
// 判成拒付 → 取消订单；判成临时故障 → 退避重试；
// 判成金额不符 → 停住等人。分类错了，钱的方向就错了。
// 所以这里用 httptest 把网关的每种响应形态都摆一遍。
func TestChargeOutcomes(t *testing.T) {
	const amount int64 = 59900

	tests := []struct {
		name       string
		statusCode int
		body       string
		want       string // ok / declined / mismatch / rejected / retryable
	}{
		{
			name:       "扣款成功",
			statusCode: http.StatusOK,
			body:       `{"order_id":"42","status":"succeeded","payment_id":"pay_1","amount_cents":59900}`,
			want:       "ok",
		},
		{
			name:       "拒付是业务终态",
			statusCode: http.StatusOK,
			body:       `{"order_id":"42","status":"failed","payment_id":"pay_2","amount_cents":59900,"failure_reason":"card_declined"}`,
			want:       "declined",
		},
		{
			// 网关幂等返回第一次的金额：价格改过之后就是这个样子
			name:       "金额不符",
			statusCode: http.StatusOK,
			body:       `{"order_id":"42","status":"succeeded","payment_id":"pay_3","amount_cents":49900}`,
			want:       "mismatch",
		},
		{
			name:       "请求本身不合法（422）",
			statusCode: http.StatusUnprocessableEntity,
			body:       `{"detail":"amount_cents must be positive"}`,
			want:       "rejected",
		},
		{
			// 限流是暂时的，绝不能当拒付去取消订单
			name:       "限流（429）",
			statusCode: http.StatusTooManyRequests,
			body:       `{"detail":"slow down"}`,
			want:       "retryable",
		},
		{
			name:       "网关 5xx",
			statusCode: http.StatusBadGateway,
			body:       `upstream boom`,
			want:       "retryable",
		},
		{
			name:       "响应不是 JSON",
			statusCode: http.StatusOK,
			body:       `<html>gateway proxy error</html>`,
			want:       "retryable",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/charge" {
					t.Errorf("请求路径 = %q，期望 /charge", r.URL.Path)
				}
				// 顺带确认请求体里的金额就是我们要求核对的金额
				var req struct {
					OrderID     string `json:"order_id"`
					AmountCents int64  `json:"amount_cents"`
				}
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Errorf("请求体解不开: %v", err)
				}
				if req.AmountCents != amount {
					t.Errorf("请求金额 = %d，期望 %d", req.AmountCents, amount)
				}
				w.WriteHeader(tc.statusCode)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer srv.Close()

			c := New(srv.URL, 2*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
			_, err := c.Charge(context.Background(), "42", amount)

			var (
				declined *DeclinedError
				mismatch *AmountMismatchError
				rejected *RequestRejectedError
			)
			var got string
			switch {
			case err == nil:
				got = "ok"
			case errors.As(err, &declined):
				got = "declined"
			case errors.As(err, &mismatch):
				got = "mismatch"
				if mismatch.Want != amount || mismatch.Got != 49900 {
					t.Fatalf("金额不符的字段不对: want=%d got=%d", mismatch.Want, mismatch.Got)
				}
			case errors.As(err, &rejected):
				got = "rejected"
				if rejected.Status != tc.statusCode {
					t.Fatalf("拒绝类型里的状态码 = %d，期望 %d", rejected.Status, tc.statusCode)
				}
			default:
				got = "retryable"
			}
			if got != tc.want {
				t.Fatalf("分类 = %q，期望 %q（err=%v）", got, tc.want, err)
			}
		})
	}
}

// 网关连不上时必须是普通 error（可重试），而不是拒付 ——
// 把网络故障当拒付会直接把订单取消。
func TestChargeNetworkErrorIsRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close() // 关掉，连接被拒

	c := New(url, 500*time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil)))
	_, err := c.Charge(context.Background(), "42", 100)
	if err == nil {
		t.Fatal("网关不可达时应返回 error")
	}
	var declined *DeclinedError
	if errors.As(err, &declined) {
		t.Fatal("连接失败被误判成拒付，会直接取消订单")
	}
}

// 没配 PAYMENT_URL 时 Enabled() 必须是 false，worker 才会退回模拟支付。
// 这条看着琐碎，但它决定了「本地只起 api+worker」能不能跑通。
func TestDisabledClient(t *testing.T) {
	c := New("", time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if c.Enabled() {
		t.Fatal("空 baseURL 时 Enabled() 应为 false")
	}
	if _, err := c.Charge(context.Background(), "1", 1); err == nil {
		t.Fatal("未配置时 Charge 应返回 error")
	}
}
