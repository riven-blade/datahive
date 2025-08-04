package core

import (
	"context"
)

// =============================================================================
// 核心业务接口
// =============================================================================

// Publisher 数据发布者接口
type Publisher interface {
	Publish(ctx context.Context, event Event) error
}

// Subscriber 数据订阅者接口
type Subscriber interface {
	Subscribe(ctx context.Context, req SubscriptionRequest) (*Subscription, error)
	Unsubscribe(ctx context.Context, id string) error
}

// EventHandler 事件处理器接口
type EventHandler interface {
	Handle(ctx context.Context, event Event) error
}

// =============================================================================
// 核心数据结构 - 更简洁的设计
// =============================================================================

// Event 统一事件结构
type Event struct {
	Type      EventType `json:"type"`
	Source    string    `json:"source"`    // 数据源：binance, bybit等
	Market    string    `json:"market"`    // 市场类型：spot, futures等
	Symbol    string    `json:"symbol"`    // 交易对
	Data      any       `json:"data"`      // 具体数据
	Timestamp int64     `json:"timestamp"` // 时间戳
}

// EventType 事件类型枚举
type EventType string

const (
	EventTicker    EventType = "ticker"
	EventKline     EventType = "kline"
	EventTrade     EventType = "trade"
	EventOrderBook EventType = "orderbook"
)

// SubscriptionRequest 订阅请求
type SubscriptionRequest struct {
	Exchange string      `json:"exchange"`
	Market   string      `json:"market"`
	Symbols  []string    `json:"symbols"`
	Events   []EventType `json:"events"`
	Options  Options     `json:"options,omitempty"`
}

// Subscription 订阅信息
type Subscription struct {
	ID        string              `json:"id"`
	Request   SubscriptionRequest `json:"request"`
	Status    SubscriptionStatus  `json:"status"`
	CreatedAt int64               `json:"created_at"`
}

// MinerSubscription 矿工订阅
type MinerSubscription struct {
	Symbol  string      `json:"symbol"`
	Events  []EventType `json:"events"`
	Options Options     `json:"options"`
}

// Options 可选配置
type Options struct {
	Interval string `json:"interval,omitempty"` // K线间隔
	Depth    int    `json:"depth,omitempty"`    // 深度档位
}

// SubscriptionStatus 订阅状态
type SubscriptionStatus string

const (
	StatusActive   SubscriptionStatus = "active"
	StatusInactive SubscriptionStatus = "inactive"
	StatusError    SubscriptionStatus = "error"
)

// DataPoint 统一数据点结构
type DataPoint struct {
	Type      EventType `json:"type"`
	Exchange  string    `json:"exchange"`
	Market    string    `json:"market"`
	Symbol    string    `json:"symbol"`
	Data      any       `json:"data"`
	Timestamp int64     `json:"timestamp"`
}

// Query 查询结构
type Query struct {
	Type      EventType `json:"type"`
	Exchange  string    `json:"exchange,omitempty"`
	Market    string    `json:"market,omitempty"`
	Symbol    string    `json:"symbol,omitempty"`
	StartTime int64     `json:"start_time,omitempty"`
	EndTime   int64     `json:"end_time,omitempty"`
	Limit     int       `json:"limit,omitempty"`
}

// Health 健康状态
type Health struct {
	Status    string            `json:"status"`  // healthy, unhealthy, degraded
	Details   map[string]string `json:"details"` // 详细信息
	Timestamp int64             `json:"timestamp"`
}
