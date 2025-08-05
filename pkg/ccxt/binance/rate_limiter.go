package binance

import (
	"context"
	"sync"
	"time"
)

// ========== 简洁优雅的Binance速率限制器 ==========

// RateLimiter Binance专用速率限制器
// 基于令牌桶算法，支持权重和请求数的双重限制
type RateLimiter struct {
	// 权重令牌桶 - Binance API权重限制
	weightBucket *tokenBucket
	// 请求令牌桶 - Binance API请求数限制
	requestBucket *tokenBucket
}

// tokenBucket 令牌桶实现
type tokenBucket struct {
	capacity   float64   // 桶容量
	tokens     float64   // 当前令牌数
	refillRate float64   // 令牌填充速率（令牌/秒）
	lastRefill time.Time // 上次填充时间
	mutex      sync.Mutex
}

// NewRateLimiter 创建Binance速率限制器
func NewRateLimiter() *RateLimiter {
	return &RateLimiter{
		// 权重限制：Binance官方每分钟1200权重
		// 设置为18权重/秒（比官方20权重/秒保守10%）
		// 突发容量540权重（30秒的量）
		weightBucket: newTokenBucket(540, 18.0),

		// 请求限制：Binance官方每秒20请求
		// 设置为18请求/秒（比官方保守10%）
		// 突发容量180请求（10秒的量）
		requestBucket: newTokenBucket(180, 18.0),
	}
}

// newTokenBucket 创建令牌桶
func newTokenBucket(capacity, refillRate float64) *tokenBucket {
	return &tokenBucket{
		capacity:   capacity,
		tokens:     capacity, // 初始满桶
		refillRate: refillRate,
		lastRefill: time.Now(),
	}
}

func (rl *RateLimiter) Allow(cost int) bool {
	// 必须同时满足权重和请求数限制
	return rl.requestBucket.tryConsume(1) && rl.weightBucket.tryConsume(float64(cost))
}

// Wait 等待直到可以执行请求
// 这是阻塞方法，会等待直到有足够的令牌
func (rl *RateLimiter) Wait(ctx context.Context, cost int) error {
	// 先等待请求数令牌
	if err := rl.requestBucket.wait(ctx, 1); err != nil {
		return err
	}

	// 再等待权重令牌
	if err := rl.weightBucket.wait(ctx, float64(cost)); err != nil {
		return err
	}

	return nil
}

func (rl *RateLimiter) GetStatus() map[string]interface{} {
	rl.weightBucket.refill()
	rl.requestBucket.refill()

	return map[string]interface{}{
		"weight_tokens":    int(rl.weightBucket.tokens),
		"weight_capacity":  int(rl.weightBucket.capacity),
		"request_tokens":   int(rl.requestBucket.tokens),
		"request_capacity": int(rl.requestBucket.capacity),
	}
}

// ========== 令牌桶内部实现 ==========

// refill 填充令牌
func (tb *tokenBucket) refill() {
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()

	// 计算应该添加的令牌数
	tokensToAdd := elapsed * tb.refillRate

	// 更新令牌数，不超过容量
	tb.tokens = min(tb.capacity, tb.tokens+tokensToAdd)
	tb.lastRefill = now
}

// tryConsume 尝试消费令牌（非阻塞）
func (tb *tokenBucket) tryConsume(tokens float64) bool {
	tb.mutex.Lock()
	defer tb.mutex.Unlock()

	tb.refill()

	if tb.tokens >= tokens {
		tb.tokens -= tokens
		return true
	}
	return false
}

// wait 等待直到有足够令牌（阻塞）
func (tb *tokenBucket) wait(ctx context.Context, tokens float64) error {
	for {
		tb.mutex.Lock()
		tb.refill()

		if tb.tokens >= tokens {
			tb.tokens -= tokens
			tb.mutex.Unlock()
			return nil
		}

		// 计算需要等待的时间
		needed := tokens - tb.tokens
		waitTime := time.Duration(needed/tb.refillRate*1000) * time.Millisecond
		tb.mutex.Unlock()

		// 限制最大等待时间，避免等待过久
		if waitTime > 5*time.Second {
			waitTime = 5 * time.Second
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitTime):
			// 继续循环重试
		}
	}
}

// ========== 辅助函数 ==========

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
