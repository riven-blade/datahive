package core

import (
	"context"
	"fmt"

	"github.com/riven-blade/datahive/pkg/protocol/pb"
	"github.com/riven-blade/datahive/pkg/server"

	"google.golang.org/protobuf/proto"
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
	// 将Event转换为协议格式并序列化为protobuf
	var data []byte
	var action pb.ActionType
	var err error

	switch event.Type {
	case EventPrice:
		action = pb.ActionType_PRICE_UPDATE
		data, err = p.serializePriceUpdate(event)
	case EventKline:
		action = pb.ActionType_KLINE_UPDATE
		data, err = p.serializeKlineUpdate(event)
	case EventTrade:
		action = pb.ActionType_TRADE_UPDATE
		data, err = p.serializeTradeUpdate(event)
	case EventOrderBook:
		action = pb.ActionType_ORDERBOOK_UPDATE
		data, err = p.serializeOrderBookUpdate(event)
	default:
		return nil // 忽略未知事件类型
	}

	if err != nil {
		return err
	}

	return p.transport.Broadcast(action, data, event.Topic)
}

// serializePriceUpdate 序列化价格更新事件
func (p *EventPublisher) serializePriceUpdate(event Event) ([]byte, error) {
	// 统一要求Data为map类型
	priceData, ok := event.Data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid price data type, expected map[string]interface{}")
	}

	// 转换为protobuf格式的轻量级价格更新
	priceUpdate := &pb.PriceUpdate{
		Topic:     event.Topic,
		Symbol:    getStringFromData(priceData, "symbol"),
		Price:     getFloat64FromData(priceData, "price"),
		Timestamp: getInt64FromData(priceData, "timestamp"),
	}

	return proto.Marshal(priceUpdate)
}

// serializeKlineUpdate 序列化K线更新事件
func (p *EventPublisher) serializeKlineUpdate(event Event) ([]byte, error) {
	klineData, ok := event.Data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid kline data type, expected map[string]interface{}")
	}

	protocolKline := &pb.Kline{
		Symbol:    getStringFromData(klineData, "symbol"),
		Exchange:  event.Source,
		Timeframe: getStringFromData(klineData, "timeframe"),
		OpenTime:  getInt64FromData(klineData, "timestamp"),
		Open:      getFloat64FromData(klineData, "open"),
		High:      getFloat64FromData(klineData, "high"),
		Low:       getFloat64FromData(klineData, "low"),
		Close:     getFloat64FromData(klineData, "close"),
		Volume:    getFloat64FromData(klineData, "volume"),
		Closed:    getBoolFromData(klineData, "closed"),
	}

	klineUpdate := &pb.KlineUpdate{
		Topic: event.Topic,
		Kline: protocolKline,
	}

	return proto.Marshal(klineUpdate)
}

// serializeTradeUpdate 序列化交易更新事件
func (p *EventPublisher) serializeTradeUpdate(event Event) ([]byte, error) {
	tradeData, ok := event.Data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid trade data type, expected map[string]interface{}")
	}

	protocolTrade := &pb.Trade{
		Id:        getStringFromData(tradeData, "id"),
		Symbol:    getStringFromData(tradeData, "symbol"),
		Exchange:  event.Source,
		Price:     getFloat64FromData(tradeData, "price"),
		Quantity:  getFloat64FromData(tradeData, "amount"),
		Side:      getStringFromData(tradeData, "side"),
		Timestamp: getInt64FromData(tradeData, "timestamp"),
	}

	tradeUpdate := &pb.TradeUpdate{
		Topic: event.Topic,
		Trade: protocolTrade,
	}

	return proto.Marshal(tradeUpdate)
}

// serializeOrderBookUpdate 序列化订单簿更新事件
func (p *EventPublisher) serializeOrderBookUpdate(event Event) ([]byte, error) {
	obData, ok := event.Data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid orderbook data type, expected map[string]interface{}")
	}

	protocolOrderBook := &pb.OrderBook{
		Symbol:    getStringFromData(obData, "symbol"),
		Timestamp: getInt64FromData(obData, "timestamp"),
		Bids:      []*pb.PriceLevel{},
		Asks:      []*pb.PriceLevel{},
	}

	// 转换bids和asks (这里需要根据实际数据结构调整)
	if bids, ok := obData["bids"].([]interface{}); ok {
		for _, bid := range bids {
			if bidLevel, ok := bid.([]float64); ok && len(bidLevel) >= 2 {
				protocolOrderBook.Bids = append(protocolOrderBook.Bids, &pb.PriceLevel{
					Price:    bidLevel[0],
					Quantity: bidLevel[1],
				})
			}
		}
	}

	if asks, ok := obData["asks"].([]interface{}); ok {
		for _, ask := range asks {
			if askLevel, ok := ask.([]float64); ok && len(askLevel) >= 2 {
				protocolOrderBook.Asks = append(protocolOrderBook.Asks, &pb.PriceLevel{
					Price:    askLevel[0],
					Quantity: askLevel[1],
				})
			}
		}
	}

	orderBookUpdate := &pb.OrderBookUpdate{
		Topic:     event.Topic,
		Orderbook: protocolOrderBook,
	}

	return proto.Marshal(orderBookUpdate)
}

// 辅助函数：数据类型转换
func getStringFromData(data map[string]interface{}, key string) string {
	if value, ok := data[key]; ok {
		if str, ok := value.(string); ok {
			return str
		}
	}
	return ""
}

func getInt64FromData(data map[string]interface{}, key string) int64 {
	if value, ok := data[key]; ok {
		switch v := value.(type) {
		case int64:
			return v
		case int:
			return int64(v)
		case float64:
			return int64(v)
		}
	}
	return 0
}

func getFloat64FromData(data map[string]interface{}, key string) float64 {
	if value, ok := data[key]; ok {
		switch v := value.(type) {
		case float64:
			return v
		case int64:
			return float64(v)
		case int:
			return float64(v)
		}
	}
	return 0.0
}

func getBoolFromData(data map[string]interface{}, key string) bool {
	if value, ok := data[key]; ok {
		if b, ok := value.(bool); ok {
			return b
		}
	}
	return false
}
