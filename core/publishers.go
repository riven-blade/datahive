package core

import (
	"context"
	"datahive/pkg/protocol"
	"datahive/server"
	"encoding/json"
)

// EventPublisher 事件发布器实现
type EventPublisher struct {
	transport server.Transport
}

// NewEventPublisher 创建事件发布器
func NewEventPublisher(transport server.Transport) *EventPublisher {
	return &EventPublisher{
		transport: transport,
	}
}

// Publish 发布事件 - 实现Publisher接口
func (p *EventPublisher) Publish(ctx context.Context, event Event) error {
	// 将Event转换为协议格式
	var action protocol.ActionType
	switch event.Type {
	case EventTicker:
		action = protocol.ActionTickerUpdate
	case EventKline:
		action = protocol.ActionKlineUpdate
	case EventTrade:
		action = protocol.ActionTradeUpdate
	case EventOrderBook:
		action = protocol.ActionOrderBookUpdate
	default:
		return nil // 忽略未知事件类型
	}

	// 序列化数据
	data, err := json.Marshal(event.Data)
	if err != nil {
		return err
	}

	// 广播事件
	return p.transport.Broadcast(action, data)
}
