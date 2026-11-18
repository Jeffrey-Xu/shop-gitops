// Package cache 是 Redis 缓存层：cache-aside + singleflight + TTL 抖动。
//
// 刻意不做的两件事：
//  1. 不用 Redis 扣库存 —— 双写不一致的代价远高于收益，库存只认 Postgres。
//  2. 不缓存订单 —— 订单是"刚写完就要读"的数据，缓存只会让前端看到旧状态。
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

const (
	KeyProductList = "shop:products:list"
	// FlagFailNext 是给演示用的开关：redis-cli SET shop:demo:fail 1 之后，
	// 下一条消息会被 worker 判为支付失败，用来演示重试和补偿。
	FlagFailNext = "shop:demo:fail"
)

func KeyProduct(id int64) string {
	return fmt.Sprintf("shop:products:%d", id)
}

type Cache struct {
	rdb *redis.Client
	ttl time.Duration
	sf  singleflight.Group
}

func New(ctx context.Context, addr, password string, ttl time.Duration) (*Cache, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	})
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	return &Cache{rdb: rdb, ttl: ttl}, nil
}

func (c *Cache) Close() error { return c.rdb.Close() }

func (c *Cache) Ping(ctx context.Context) error { return c.rdb.Ping(ctx).Err() }

// GetJSON 命中返回 true。未命中不是错误，别把 redis.Nil 当异常往上抛。
func (c *Cache) GetJSON(ctx context.Context, key string, dst any) (bool, error) {
	b, err := c.rdb.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return false, nil
		}
		return false, err
	}
	if err := json.Unmarshal(b, dst); err != nil {
		// 缓存里是脏数据：删掉当未命中处理，别让一个坏值毒化整个 key
		_ = c.rdb.Del(ctx, key).Err()
		return false, nil
	}
	return true, nil
}

func (c *Cache) SetJSON(ctx context.Context, key string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.rdb.Set(ctx, key, b, c.ttlWithJitter()).Err()
}

// ttlWithJitter 加 0~20% 随机，避免同一时刻写入的 key 集体过期造成击穿。
func (c *Cache) ttlWithJitter() time.Duration {
	jitter := c.ttl / 5
	if jitter <= 0 {
		return c.ttl
	}
	return c.ttl + time.Duration(rand.Int63n(int64(jitter)))
}

// Do 用 singleflight 把并发回源合并成一次。
// 100 个请求同时未命中，只会打 1 次数据库。
func (c *Cache) Do(key string, fn func() (any, error)) (any, error) {
	v, err, _ := c.sf.Do(key, fn)
	return v, err
}

func (c *Cache) Del(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	return c.rdb.Del(ctx, keys...).Err()
}

func (c *Cache) Incr(ctx context.Context, key string) {
	_ = c.rdb.Incr(ctx, key).Err()
}

func (c *Cache) GetInt(ctx context.Context, key string) int64 {
	s, err := c.rdb.Get(ctx, key).Result()
	if err != nil {
		return 0
	}
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

// FlagSet 判断演示开关是否打开。用 GET 而不是 GETDEL：
// 开关要保持到人工 DEL 为止，这样才能把「重试 3 次后进 DLQ」完整演一遍。
// 清理：redis-cli DEL shop:demo:fail
func (c *Cache) FlagSet(ctx context.Context, key string) bool {
	n, err := c.rdb.Exists(ctx, key).Result()
	return err == nil && n > 0
}

// QueueDepth 之类的采样留给 broker，缓存层不掺和。
