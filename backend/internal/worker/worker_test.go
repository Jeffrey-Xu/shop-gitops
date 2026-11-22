package worker

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/broker"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/config"
)

// retryDelay 是纯函数，所以这里不需要 PG、不需要 broker ——
// 而这正是它值得被测试的原因：改一个系数，整条重试曲线的形状就变了，
// 而「延迟不对」在集成测试里表现为「等得有点久」，很难断言。
//
// 断言的是**区间**而不是某个具体值：函数里有随机抖动，
// 写死期望值等于把测试变成随机失败的机器。
func TestRetryDelayBounds(t *testing.T) {
	const base = 2 * time.Second
	w := &Worker{Cfg: config.Config{RetryBaseDelay: base}}
	limit := time.Duration(broker.RetryDelayMS) * time.Millisecond

	cases := []struct {
		attempts int
		min, max time.Duration
	}{
		// 第 1 次失败：2s × [0.5, 1.5) → [1s, 3s)
		{attempts: 0, min: 1 * time.Second, max: 3 * time.Second},
		// 第 2 次失败：4s × [0.5, 1.5) → [2s, 6s)
		{attempts: 1, min: 2 * time.Second, max: 6 * time.Second},
	}
	for _, tc := range cases {
		for i := 0; i < 200; i++ {
			d := w.retryDelay(tc.attempts)
			if d < tc.min || d > tc.max {
				t.Fatalf("attempts=%d 延迟 %v 落在 [%v, %v] 之外", tc.attempts, d, tc.min, tc.max)
			}
			if d > limit {
				t.Fatalf("attempts=%d 延迟 %v 超过队列 TTL %v，会被 broker 截断", tc.attempts, d, limit)
			}
		}
	}
}

// 抖动必须真的存在：同一批订单在同一秒失败，如果延迟是常量，
// 它们会在同一秒整批回来 —— 那不是重试，是把一次抖动放大成三波尖峰。
func TestRetryDelayIsJittered(t *testing.T) {
	w := &Worker{Cfg: config.Config{RetryBaseDelay: 2 * time.Second}}
	seen := make(map[time.Duration]bool, 16)
	for i := 0; i < 100; i++ {
		seen[w.retryDelay(0)] = true
	}
	if len(seen) < 2 {
		t.Fatalf("延迟没有任何抖动：100 次只出现 %d 个取值", len(seen))
	}
}

// base 配得很大时，结果必须被封顶在队列 TTL 以内。
// per-message Expiration 大于队列 x-message-ttl 会被 broker 截断，
// 那种「代码里写着 30 秒、实际 10 秒」的偏差只能靠这条测试兜住。
func TestRetryDelayCapsAtQueueTTL(t *testing.T) {
	w := &Worker{Cfg: config.Config{RetryBaseDelay: 30 * time.Second}}
	limit := time.Duration(broker.RetryDelayMS) * time.Millisecond
	for i := 0; i < 100; i++ {
		if d := w.retryDelay(3); d > limit {
			t.Fatalf("延迟 %v 超过队列 TTL %v", d, limit)
		}
	}
}

// 没配置时也要有个能用的默认值：RetryBaseDelay 为 0 会让 d 永远是 0，
// 消息立刻回来，退避等于不存在。
func TestRetryDelayDefaultBase(t *testing.T) {
	w := &Worker{}
	if d := w.retryDelay(0); d < 500*time.Millisecond {
		t.Fatalf("默认 base 下延迟只有 %v，等于没有退避", d)
	}
}

// chargeNote 由 paymentID 单独决定：重试时 note 是照着消息头里的
// payment_id 重新拼的，两处格式一旦漂移，order_events 里同一个单
// 会出现两种文案。
func TestChargeNote(t *testing.T) {
	cases := []struct {
		paymentID string
		want      string
	}{
		{"sim_42", "worker: payment captured (simulated)"},
		{"pay_abc123", "worker: payment captured (pay_abc123)"},
	}
	for _, tc := range cases {
		if got := chargeNote(tc.paymentID); got != tc.want {
			t.Fatalf("chargeNote(%q) = %q，期望 %q", tc.paymentID, got, tc.want)
		}
	}
}

// 消息头经过一次 JSON 往返、或者被别的语言投递时，类型会变。
// 读错的后果不对称：payment_id 读成空串会再调一次网关（幂等兜住了，
// 但白跑一趟，日志也会骗人）；attempts 读成 0 会让重试永远从第 1 次开始。
func TestHeaderReaders(t *testing.T) {
	ints := map[string]any{
		"int32":   int32(3),
		"int64":   int64(4),
		"int":     5,
		"string":  "6", // 字符串不认 —— 宁可为 0 也不要猜
		"missing": nil,
	}
	for key, want := range map[string]int{"int32": 3, "int64": 4, "int": 5, "string": 0} {
		if got := headerInt(amqp.Table(ints), key); got != want {
			t.Fatalf("headerInt(%s) = %d，期望 %d", key, got, want)
		}
	}
	if got := headerInt(nil, "int32"); got != 0 {
		t.Fatalf("nil 表应返回 0，实际 %d", got)
	}

	strs := amqp.Table{
		"string": "pay_1",
		"bytes":  []byte("pay_2"),
		"number": 7,
	}
	for key, want := range map[string]string{"string": "pay_1", "bytes": "pay_2", "number": ""} {
		if got := headerString(strs, key); got != want {
			t.Fatalf("headerString(%s) = %q，期望 %q", key, got, want)
		}
	}
	if got := headerString(nil, "string"); got != "" {
		t.Fatalf("nil 表应返回空串，实际 %q", got)
	}
}
