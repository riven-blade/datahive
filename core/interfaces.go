package core

import (
	"context"

	"github.com/mitchellh/mapstructure"
)

// =============================================================================
// 核心业务接口
// =============================================================================

// Publisher 数据发布者接口
type Publisher interface {
	Publish(ctx context.Context, event Event) error
}

// EventHandler 事件处理器接口
type EventHandler interface {
	Handle(ctx context.Context, event Event) error
}

// =============================================================================
// 核心数据结构
// =============================================================================

// Event 统一事件结构
type Event struct {
	Type      EventType `json:"type"`      // 事件类型
	Source    string    `json:"source"`    // 数据源：binance, bybit等
	Market    string    `json:"market"`    // 市场类型：spot, futures等
	Symbol    string    `json:"symbol"`    // 交易对（原始格式，如BTC/USDT）
	Topic     string    `json:"Topic"`     // topic
	Data      any       `json:"data"`      // 具体数据
	Timestamp int64     `json:"timestamp"` // 时间戳
}

// EventType 事件类型枚举
type EventType string

const (
	EventMiniTicker EventType = "miniticker" // 轻量级ticker数据
	EventBookTicker EventType = "bookticker" // 最优买卖价数据
	EventFullTicker EventType = "ticker"     // 完整ticker数据
	EventKline      EventType = "kline"      // K线数据
	EventTrade      EventType = "trade"      // 交易数据
	EventOrderBook  EventType = "order_book" // 订单簿数据
	EventMarkPrice  EventType = "mark_price" // 标记价格数据(仅期货)
)

// SubscriptionRequest 订阅请求
type SubscriptionRequest struct {
	Exchange string    `json:"exchange"`
	Market   string    `json:"market"`
	Symbol   string    `json:"symbol"`
	Event    EventType `json:"event"`
	Options  Options   `json:"options,omitempty"`
}

// MinerSubscription 矿工订阅
type MinerSubscription struct {
	Symbol     string    `json:"symbol"`             // 用户请求的原始symbol (如 BTC/USDT)
	Event      EventType `json:"event"`              // 单个事件类型
	Interval   string    `json:"interval,omitempty"` // K线间隔 (仅kline)
	Depth      int       `json:"depth,omitempty"`    // 深度档位 (仅orderbook)
	Topic      string    `json:"topic"`              // 生成的topic作为订阅ID
	StreamName string    `json:"stream_name"`        // stream name

	// 新架构：专用channel和订阅管理
	SubscriptionID string      `json:"subscription_id"` // WebSocket订阅ID，用于取消订阅
	DataChannel    interface{} `json:"-"`               // 专用数据channel，根据Event类型存储不同类型的channel
	CancelFunc     func()      `json:"-"`               // 取消该订阅的函数
}

func (m *MinerSubscription) ToMap() map[string]interface{} {
	result := make(map[string]interface{})
	config := &mapstructure.DecoderConfig{
		TagName: "json",
		Result:  &result,
	}
	decoder, _ := mapstructure.NewDecoder(config)
	_ = decoder.Decode(m)
	return result
}

func (m *MinerSubscription) FromMap(data map[string]interface{}) error {
	return mapstructure.Decode(data, m)
}

// GetChannelParams 将订阅参数转换为 channel 生成所需的参数
func (m *MinerSubscription) GetChannelParams() map[string]interface{} {
	params := map[string]interface{}{
		"eventType": string(m.Event),
	}

	if m.Interval != "" {
		params["interval"] = m.Interval
	}
	if m.Depth > 0 {
		params["depth"] = m.Depth
	}

	return params
}

// Options 可选配置
type Options struct {
	Interval string `json:"interval,omitempty"` // K线间隔
	Depth    int    `json:"depth,omitempty"`    // 深度档位
}

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
