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
	case EventMiniTicker:
		action = pb.ActionType_MINI_TICKER_UPDATE
		data, err = p.serializeMiniTickerUpdate(event)
	case EventMarkPrice:
		action = pb.ActionType_MARK_PRICE_UPDATE
		data, err = p.serializeMarkPriceUpdate(event)
	case EventBookTicker:
		action = pb.ActionType_BOOK_TICKER_UPDATE
		data, err = p.serializeBookTickerUpdate(event)
	case EventTicker:
		action = pb.ActionType_FULL_TICKER_UPDATE
		data, err = p.serializeFullTickerUpdate(event)
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

	// 创建独立的 Price 结构体
	price := &pb.Price{
		Symbol:    getStringFromData(priceData, "symbol"),
		Price:     getFloat64FromData(priceData, "price"),
		Timestamp: getInt64FromData(priceData, "timestamp"),
	}

	// 转换为protobuf格式的轻量级价格更新
	priceUpdate := &pb.PriceUpdate{
		Topic: event.Topic,
		Price: price,
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

// =====================================================================================
// 新增的序列化方法 - 支持增强协议
// =====================================================================================

// serializeMiniTickerUpdate 序列化轻量级ticker更新事件
func (p *EventPublisher) serializeMiniTickerUpdate(event Event) ([]byte, error) {
	tickerData, ok := event.Data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid mini ticker data type, expected map[string]interface{}")
	}

	miniTicker := &pb.MiniTicker{
		Symbol:      getStringFromData(tickerData, "symbol"),
		Timestamp:   getInt64FromData(tickerData, "timestamp"),
		Open:        getFloat64FromData(tickerData, "open"),
		High:        getFloat64FromData(tickerData, "high"),
		Low:         getFloat64FromData(tickerData, "low"),
		Close:       getFloat64FromData(tickerData, "close"),
		Volume:      getFloat64FromData(tickerData, "volume"),
		QuoteVolume: getFloat64FromData(tickerData, "quote_volume"),
	}

	miniTickerUpdate := &pb.MiniTickerUpdate{
		Topic:        event.Topic,
		MiniTicker:   miniTicker,
		SourceStream: getStringFromData(tickerData, "stream_name"),
	}

	return proto.Marshal(miniTickerUpdate)
}

// serializeMarkPriceUpdate 序列化标记价格更新事件
func (p *EventPublisher) serializeMarkPriceUpdate(event Event) ([]byte, error) {
	markPriceData, ok := event.Data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid mark price data type, expected map[string]interface{}")
	}

	markPrice := &pb.MarkPrice{
		Symbol:               getStringFromData(markPriceData, "symbol"),
		Timestamp:            getInt64FromData(markPriceData, "timestamp"),
		MarkPrice:            getFloat64FromData(markPriceData, "mark_price"),
		IndexPrice:           getFloat64FromData(markPriceData, "index_price"),
		EstimatedSettlePrice: getFloat64FromData(markPriceData, "estimated_settle_price"),
		FundingRate:          getFloat64FromData(markPriceData, "funding_rate"),
		FundingTime:          getInt64FromData(markPriceData, "funding_time"),
	}

	markPriceUpdate := &pb.MarkPriceUpdate{
		Topic:        event.Topic,
		MarkPrice:    markPrice,
		SourceStream: getStringFromData(markPriceData, "stream_name"),
	}

	return proto.Marshal(markPriceUpdate)
}

// serializeBookTickerUpdate 序列化最优买卖价更新事件
func (p *EventPublisher) serializeBookTickerUpdate(event Event) ([]byte, error) {
	bookTickerData, ok := event.Data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid book ticker data type, expected map[string]interface{}")
	}

	bookTicker := &pb.BookTicker{
		Symbol:      getStringFromData(bookTickerData, "symbol"),
		Timestamp:   getInt64FromData(bookTickerData, "timestamp"),
		BidPrice:    getFloat64FromData(bookTickerData, "bid_price"),
		BidQuantity: getFloat64FromData(bookTickerData, "bid_quantity"),
		AskPrice:    getFloat64FromData(bookTickerData, "ask_price"),
		AskQuantity: getFloat64FromData(bookTickerData, "ask_quantity"),
	}

	bookTickerUpdate := &pb.BookTickerUpdate{
		Topic:        event.Topic,
		BookTicker:   bookTicker,
		SourceStream: getStringFromData(bookTickerData, "stream_name"),
	}

	return proto.Marshal(bookTickerUpdate)
}

// serializeFullTickerUpdate 序列化完整ticker更新事件
func (p *EventPublisher) serializeFullTickerUpdate(event Event) ([]byte, error) {
	tickerData, ok := event.Data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid full ticker data type, expected map[string]interface{}")
	}

	ticker := &pb.Ticker{
		Symbol:        getStringFromData(tickerData, "symbol"),
		Timestamp:     getInt64FromData(tickerData, "timestamp"),
		Last:          getFloat64FromData(tickerData, "last"),
		Bid:           getFloat64FromData(tickerData, "bid"),
		Ask:           getFloat64FromData(tickerData, "ask"),
		BidVolume:     getFloat64FromData(tickerData, "bid_volume"),
		AskVolume:     getFloat64FromData(tickerData, "ask_volume"),
		High:          getFloat64FromData(tickerData, "high"),
		Low:           getFloat64FromData(tickerData, "low"),
		Open:          getFloat64FromData(tickerData, "open"),
		Close:         getFloat64FromData(tickerData, "close"),
		Volume:        getFloat64FromData(tickerData, "volume"),
		QuoteVolume:   getFloat64FromData(tickerData, "quote_volume"),
		Change:        getFloat64FromData(tickerData, "change"),
		ChangePercent: getFloat64FromData(tickerData, "change_percent"),
		Vwap:          getFloat64FromData(tickerData, "vwap"),
		MarkPrice:     getFloat64FromData(tickerData, "mark_price"),
		IndexPrice:    getFloat64FromData(tickerData, "index_price"),
		FundingRate:   getFloat64FromData(tickerData, "funding_rate"),
		FundingTime:   getInt64FromData(tickerData, "funding_time"),
	}

	tickerUpdate := &pb.FullTickerUpdate{
		Topic:  event.Topic,
		Ticker: ticker,
	}

	return proto.Marshal(tickerUpdate)
}
