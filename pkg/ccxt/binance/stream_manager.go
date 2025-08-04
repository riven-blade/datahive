package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/riven-blade/datahive/pkg/ccxt"
	"github.com/riven-blade/datahive/pkg/logger"
	"github.com/riven-blade/datahive/pkg/utils"

	"github.com/spf13/cast"
	"go.uber.org/zap"
)

// StreamManager 管理各个数据流的处理
type StreamManager struct {
	mu sync.RWMutex

	// 每个stream的处理channel
	streamChannels map[string]chan []byte // key: "btcusdt@ticker"

	// 每个stream的订阅者
	streamSubscribers map[string]*StreamSubscribers

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
}

// StreamSubscribers 单个stream的订阅者管理
type StreamSubscribers struct {
	streamName string
	dataType   string // ticker, depth, trade, kline
	symbol     string
	timeframe  string // for kline only

	// 订阅者channels
	tickerSubs  []chan *ccxt.WatchPrice
	depthSubs   []chan *ccxt.WatchOrderBook
	tradeSubs   []chan *ccxt.WatchTrade
	klineSubs   []chan *ccxt.WatchOHLCV
	balanceSubs []chan *ccxt.WatchBalance
	orderSubs   []chan *ccxt.WatchOrder

	// 订阅者ID映射
	subscriptionMap map[string]interface{} // subscriptionID -> channel

	mu sync.RWMutex
}

// NewStreamManager 创建流管理器
func NewStreamManager() *StreamManager {
	ctx, cancel := context.WithCancel(context.Background())

	return &StreamManager{
		streamChannels:    make(map[string]chan []byte),
		streamSubscribers: make(map[string]*StreamSubscribers),
		ctx:               ctx,
		cancel:            cancel,
	}
}

// RegisterStream 注册一个新的数据流
func (sm *StreamManager) RegisterStream(streamName string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.streamChannels[streamName]; exists {
		return // 已经存在
	}

	// 创建stream channel
	streamChan := make(chan []byte, 1000) // 大缓冲区
	sm.streamChannels[streamName] = streamChan

	// 解析stream信息
	symbol, dataType, timeframe := sm.parseStreamName(streamName)

	// 创建订阅者管理器
	subscribers := &StreamSubscribers{
		streamName:      streamName,
		dataType:        dataType,
		symbol:          symbol,
		timeframe:       timeframe,
		tickerSubs:      make([]chan *ccxt.WatchPrice, 0),
		depthSubs:       make([]chan *ccxt.WatchOrderBook, 0),
		tradeSubs:       make([]chan *ccxt.WatchTrade, 0),
		klineSubs:       make([]chan *ccxt.WatchOHLCV, 0),
		balanceSubs:     make([]chan *ccxt.WatchBalance, 0),
		orderSubs:       make([]chan *ccxt.WatchOrder, 0),
		subscriptionMap: make(map[string]interface{}),
	}

	sm.streamSubscribers[streamName] = subscribers

	// 启动stream处理协程
	go sm.processStream(streamName, streamChan, subscribers)

	logger.Debug("Registered new stream",
		zap.String("stream", streamName),
		zap.String("symbol", symbol),
		zap.String("data_type", dataType))
}

// SubscribeToStream 订阅数据流
func (sm *StreamManager) SubscribeToStream(streamName string, dataType string) (string, interface{}, error) {
	sm.RegisterStream(streamName) // 确保stream已注册

	sm.mu.RLock()
	subscribers, exists := sm.streamSubscribers[streamName]
	sm.mu.RUnlock()

	if !exists {
		return "", nil, fmt.Errorf("stream not found: %s", streamName)
	}

	return subscribers.Subscribe(dataType)
}

// RouteMessage 将WebSocket消息路由到对应的stream
func (sm *StreamManager) RouteMessage(rawMessage []byte) error {
	var msg map[string]interface{}
	if err := json.Unmarshal(rawMessage, &msg); err != nil {
		return err
	}

	// 处理多路复用格式: {"stream": "btcusdt@ticker", "data": {...}}
	if stream, ok := msg["stream"].(string); ok {
		return sm.routeToStream(stream, rawMessage)
	}

	// 处理单独订阅格式: {"e": "trade", "s": "BTCUSDT", ...}
	eventType, hasEvent := msg["e"].(string)
	symbol, hasSymbol := msg["s"].(string)

	if !hasEvent || !hasSymbol {
		logger.Debug("Unknown message format, ignoring", zap.ByteString("message", rawMessage))
		return nil // 忽略未知格式的消息
	}

	// 根据事件类型和交易对构造stream name
	streamName := sm.constructStreamName(strings.ToLower(symbol), eventType, msg)

	logger.Debug("Constructed stream name from message",
		zap.String("event", eventType),
		zap.String("symbol", symbol),
		zap.String("stream_name", streamName))

	return sm.routeToStream(streamName, rawMessage)
}

// routeToStream 将消息路由到指定的stream
func (sm *StreamManager) routeToStream(streamName string, rawMessage []byte) error {
	sm.mu.RLock()
	streamChan, exists := sm.streamChannels[streamName]
	sm.mu.RUnlock()

	if !exists {
		// 未注册的stream，记录但不报错
		logger.Debug("Received message for unregistered stream", zap.String("stream", streamName))
		return nil
	}

	// 非阻塞发送到stream channel
	select {
	case streamChan <- rawMessage:
		logger.Debug("Message routed to stream", zap.String("stream", streamName))
	default:
		logger.Warn("Stream channel full, dropping message", zap.String("stream", streamName))
	}

	return nil
}

// constructStreamName 根据事件类型构造stream name
func (sm *StreamManager) constructStreamName(symbol, eventType string, msg map[string]interface{}) string {
	switch eventType {
	case "trade":
		return fmt.Sprintf("%s@trade", symbol)
	case "24hrTicker":
		return fmt.Sprintf("%s@ticker", symbol)
	case "kline":
		// 从kline数据中获取interval
		if klineData, ok := msg["k"].(map[string]interface{}); ok {
			if interval, ok := klineData["i"].(string); ok {
				return fmt.Sprintf("%s@kline_%s", symbol, interval)
			}
		}
		return fmt.Sprintf("%s@kline_1m", symbol) // 默认1m
	case "depthUpdate":
		return fmt.Sprintf("%s@depth", symbol)
	case "outboundAccountPosition", "balanceUpdate":
		// 用户余额事件不依赖于特定交易对
		return "user@balance"
	case "executionReport":
		// 订单执行报告可以按交易对分组，也可以统一处理
		if symbol != "" {
			return fmt.Sprintf("%s@orders", symbol)
		}
		return "user@orders"
	default:
		return fmt.Sprintf("%s@%s", symbol, eventType)
	}
}

// processStream 处理单个stream的数据
func (sm *StreamManager) processStream(streamName string, streamChan <-chan []byte, subscribers *StreamSubscribers) {
	logger.Info("Started stream processor", zap.String("stream", streamName))

	for {
		select {
		case <-sm.ctx.Done():
			return
		case rawMessage := <-streamChan:
			if err := sm.processStreamMessage(rawMessage, subscribers); err != nil {
				logger.Error("Failed to process stream message",
					zap.String("stream", streamName),
					zap.Error(err))
			}
		}
	}
}

// processStreamMessage 处理stream消息并分发
func (sm *StreamManager) processStreamMessage(rawMessage []byte, subscribers *StreamSubscribers) error {
	var msg map[string]interface{}
	if err := json.Unmarshal(rawMessage, &msg); err != nil {
		return err
	}

	// 处理多路复用格式: {"stream": "xxx", "data": {...}}
	var data map[string]interface{}
	if dataField, ok := msg["data"].(map[string]interface{}); ok {
		data = dataField
	} else {
		// 处理单独订阅格式: 消息本身就是数据
		data = msg
	}

	switch subscribers.dataType {
	case "ticker":
		return sm.processTicker(data, subscribers)
	case "depth":
		return sm.processDepth(data, subscribers)
	case "trade":
		return sm.processTrade(data, subscribers)
	case "kline":
		return sm.processKline(data, subscribers)
	case "balance":
		return sm.processBalance(data, subscribers)
	case "orders":
		return sm.processOrder(data, subscribers)
	default:
		return fmt.Errorf("unknown data type: %s", subscribers.dataType)
	}
}

// Subscribe 订阅stream（StreamSubscribers的方法）
func (ss *StreamSubscribers) Subscribe(dataType string) (string, interface{}, error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	subscriptionID := fmt.Sprintf("%s_%s_%d", ss.streamName, dataType, time.Now().UnixNano())

	switch dataType {
	case "ticker":
		userChan := make(chan *ccxt.WatchPrice, 1000)
		ss.tickerSubs = append(ss.tickerSubs, userChan)
		ss.subscriptionMap[subscriptionID] = userChan
		return subscriptionID, userChan, nil

	case "depth":
		userChan := make(chan *ccxt.WatchOrderBook, 500)
		ss.depthSubs = append(ss.depthSubs, userChan)
		ss.subscriptionMap[subscriptionID] = userChan
		return subscriptionID, userChan, nil

	case "trade":
		userChan := make(chan *ccxt.WatchTrade, 1000)
		ss.tradeSubs = append(ss.tradeSubs, userChan)
		ss.subscriptionMap[subscriptionID] = userChan
		return subscriptionID, userChan, nil

	case "kline":
		userChan := make(chan *ccxt.WatchOHLCV, 500)
		ss.klineSubs = append(ss.klineSubs, userChan)
		ss.subscriptionMap[subscriptionID] = userChan
		return subscriptionID, userChan, nil

	case "balance":
		userChan := make(chan *ccxt.WatchBalance, 100)
		ss.balanceSubs = append(ss.balanceSubs, userChan)
		ss.subscriptionMap[subscriptionID] = userChan
		return subscriptionID, userChan, nil

	case "orders":
		userChan := make(chan *ccxt.WatchOrder, 100)
		ss.orderSubs = append(ss.orderSubs, userChan)
		ss.subscriptionMap[subscriptionID] = userChan
		return subscriptionID, userChan, nil

	default:
		return "", nil, fmt.Errorf("unsupported data type: %s", dataType)
	}
}

// parseStreamName 解析stream名称
// 例如: "btcusdt@ticker" -> symbol="BTCUSDT", dataType="ticker", timeframe=""
// 例如: "btcusdt@kline_1m" -> symbol="BTCUSDT", dataType="kline", timeframe="1m"
// 例如: "user@balance" -> symbol="", dataType="balance", timeframe=""
// 例如: "user@orders" -> symbol="", dataType="orders", timeframe=""
func (sm *StreamManager) parseStreamName(streamName string) (symbol, dataType, timeframe string) {
	parts := strings.Split(streamName, "@")
	if len(parts) != 2 {
		return "", "", ""
	}

	symbolPart := parts[0]
	streamType := parts[1]

	// 处理用户数据流
	if symbolPart == "user" {
		return "", streamType, ""
	}

	// 处理市场数据流
	symbol = strings.ToUpper(symbolPart)

	if strings.HasPrefix(streamType, "kline_") {
		dataType = "kline"
		timeframe = strings.TrimPrefix(streamType, "kline_")
	} else {
		dataType = streamType
		timeframe = ""
	}

	return symbol, dataType, timeframe
}

// 数据处理方法
func (sm *StreamManager) processTicker(data map[string]interface{}, subscribers *StreamSubscribers) error {
	logger.Debug("Processing ticker data",
		zap.String("stream", subscribers.streamName),
		zap.Int("subscribers", len(subscribers.tickerSubs)))

	timestamp := utils.SafeGetInt64WithDefault(data, "E", time.Now().UnixMilli())

	ticker := &ccxt.WatchPrice{
		Symbol:     subscribers.symbol,
		TimeStamp:  timestamp,
		Price:      utils.SafeGetFloatWithDefault(data, "c", 0),
		StreamName: subscribers.streamName, // StreamName name
	}

	// 分发给所有订阅者
	subscribers.mu.RLock()
	tickerSubs := subscribers.tickerSubs
	subscribers.mu.RUnlock()

	for _, ch := range tickerSubs {
		select {
		case ch <- ticker:
		default:
			// channel满了就跳过
		}
	}

	return nil
}

func (sm *StreamManager) processDepth(data map[string]interface{}, subscribers *StreamSubscribers) error {
	timestamp := utils.SafeGetInt64WithDefault(data, "E", time.Now().UnixMilli())

	// 解析 bids 数据
	bidPrices := make([]float64, 0)
	bidSizes := make([]float64, 0)
	if bidsData, ok := data["b"].([]interface{}); ok {
		for _, bidItem := range bidsData {
			if bidArray, ok := bidItem.([]interface{}); ok && len(bidArray) >= 2 {
				if priceStr, ok := bidArray[0].(string); ok {
					if price, err := strconv.ParseFloat(priceStr, 64); err == nil && price > 0 {
						bidPrices = append(bidPrices, price)
					}
				}
				if sizeStr, ok := bidArray[1].(string); ok {
					if size, err := strconv.ParseFloat(sizeStr, 64); err == nil && size > 0 {
						bidSizes = append(bidSizes, size)
					}
				}
			}
		}
	}

	// 解析 asks 数据
	askPrices := make([]float64, 0)
	askSizes := make([]float64, 0)
	if asksData, ok := data["a"].([]interface{}); ok {
		for _, askItem := range asksData {
			if askArray, ok := askItem.([]interface{}); ok && len(askArray) >= 2 {
				if priceStr, ok := askArray[0].(string); ok {
					if price, err := strconv.ParseFloat(priceStr, 64); err == nil && price > 0 {
						askPrices = append(askPrices, price)
					}
				}
				if sizeStr, ok := askArray[1].(string); ok {
					if size, err := strconv.ParseFloat(sizeStr, 64); err == nil && size > 0 {
						askSizes = append(askSizes, size)
					}
				}
			}
		}
	}

	orderBook := &ccxt.WatchOrderBook{
		OrderBook: ccxt.OrderBook{
			Symbol:    subscribers.symbol,
			TimeStamp: timestamp,
			Datetime:  time.Unix(timestamp/1000, (timestamp%1000)*1000000).UTC().Format(time.RFC3339),
			Bids:      ccxt.OrderBookSide{Price: bidPrices, Size: bidSizes},
			Asks:      ccxt.OrderBookSide{Price: askPrices, Size: askSizes},
			Nonce:     utils.SafeGetInt64WithDefault(data, "u", 0), // Final update ID
			Info:      data,
		},
		StreamName: subscribers.streamName, // 设置StreamName为stream name
	}

	// 分发给所有订阅者
	subscribers.mu.RLock()
	depthSubs := subscribers.depthSubs
	subscribers.mu.RUnlock()

	for _, ch := range depthSubs {
		select {
		case ch <- orderBook:
		default:
			// channel满了就跳过
		}
	}

	return nil
}

func (sm *StreamManager) processTrade(data map[string]interface{}, subscribers *StreamSubscribers) error {
	logger.Debug("Processing trade data",
		zap.String("stream", subscribers.streamName),
		zap.Int("subscribers", len(subscribers.tradeSubs)))

	timestamp := utils.SafeGetInt64WithDefault(data, "T", time.Now().UnixMilli())

	trade := &ccxt.WatchTrade{
		Trade: ccxt.Trade{
			Symbol:    subscribers.symbol,
			Timestamp: timestamp,
			Datetime:  time.Unix(timestamp/1000, (timestamp%1000)*1000000).UTC().Format(time.RFC3339),
			ID:        cast.ToString(data["t"]),
			Amount:    utils.SafeGetFloatWithDefault(data, "q", 0),
			Price:     utils.SafeGetFloatWithDefault(data, "p", 0),
			Side:      cast.ToString(data["m"]), // true for buy, false for sell
			Info:      data,
		},
		StreamName: subscribers.streamName, // 设置StreamName为stream name
	}

	// 分发给所有订阅者
	subscribers.mu.RLock()
	tradeSubs := subscribers.tradeSubs
	subscribers.mu.RUnlock()

	for _, ch := range tradeSubs {
		select {
		case ch <- trade:
		default:
			// channel满了就跳过
		}
	}

	return nil
}

func (sm *StreamManager) processKline(data map[string]interface{}, subscribers *StreamSubscribers) error {
	// 从 kline 数据中解析
	klineData, ok := data["k"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid kline data format")
	}

	timestamp := utils.SafeGetInt64WithDefault(klineData, "t", time.Now().UnixMilli())

	ohlcv := &ccxt.WatchOHLCV{
		OHLCV: ccxt.OHLCV{
			Timestamp: timestamp,
			Open:      utils.SafeGetFloatWithDefault(klineData, "o", 0),
			High:      utils.SafeGetFloatWithDefault(klineData, "h", 0),
			Low:       utils.SafeGetFloatWithDefault(klineData, "l", 0),
			Close:     utils.SafeGetFloatWithDefault(klineData, "c", 0),
			Volume:    utils.SafeGetFloatWithDefault(klineData, "v", 0),
		},
		Symbol:     subscribers.symbol,
		Timeframe:  subscribers.timeframe,
		StreamName: subscribers.streamName, // 设置StreamName为stream name
	}

	// 分发给所有订阅者
	subscribers.mu.RLock()
	klineSubs := subscribers.klineSubs
	subscribers.mu.RUnlock()

	for _, ch := range klineSubs {
		select {
		case ch <- ohlcv:
		default:
			// channel满了就跳过
		}
	}

	return nil
}

func (sm *StreamManager) processBalance(data map[string]interface{}, subscribers *StreamSubscribers) error {
	logger.Debug("Processing balance data",
		zap.String("stream", subscribers.streamName),
		zap.Int("subscribers", len(subscribers.balanceSubs)))

	// 使用现有的解析器解析余额数据
	binanceInstance := &Binance{} // 临时实例用于解析
	account := binanceInstance.parseAccountBalance(data)
	if account == nil {
		return fmt.Errorf("failed to parse account balance data")
	}

	// 转换为 WatchBalance 格式
	watchBalance := &ccxt.WatchBalance{
		Account:    *account,
		StreamName: subscribers.streamName,
	}

	// 分发给所有订阅者
	subscribers.mu.RLock()
	balanceSubs := subscribers.balanceSubs
	subscribers.mu.RUnlock()

	for _, ch := range balanceSubs {
		select {
		case ch <- watchBalance:
		default:
			// channel满了就跳过
		}
	}

	return nil
}

func (sm *StreamManager) processOrder(data map[string]interface{}, subscribers *StreamSubscribers) error {
	logger.Debug("Processing order data",
		zap.String("stream", subscribers.streamName),
		zap.Int("subscribers", len(subscribers.orderSubs)))

	// 使用现有的解析器解析订单数据
	binanceInstance := &Binance{} // 临时实例用于解析
	order := binanceInstance.parseOrderUpdate(data)
	if order == nil {
		return fmt.Errorf("failed to parse order update data")
	}

	// 转换为 WatchOrder 格式
	watchOrder := &ccxt.WatchOrder{
		Order:      *order,
		StreamName: subscribers.streamName,
	}

	// 分发给所有订阅者
	subscribers.mu.RLock()
	orderSubs := subscribers.orderSubs
	subscribers.mu.RUnlock()

	for _, ch := range orderSubs {
		select {
		case ch <- watchOrder:
		default:
			// channel满了就跳过
		}
	}

	return nil
}

// Unsubscribe 取消订阅
func (sm *StreamManager) Unsubscribe(subscriptionID string) error {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	// 遍历所有stream找到对应的订阅
	for _, subscribers := range sm.streamSubscribers {
		if err := subscribers.RemoveSubscription(subscriptionID); err == nil {
			return nil // 找到并移除成功
		}
	}

	return fmt.Errorf("subscription not found: %s", subscriptionID)
}

// RemoveSubscription 移除订阅（StreamSubscribers的方法）
func (ss *StreamSubscribers) RemoveSubscription(subscriptionID string) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	ch, exists := ss.subscriptionMap[subscriptionID]
	if !exists {
		return fmt.Errorf("subscription not found")
	}

	// 根据channel类型找到并移除
	switch ch := ch.(type) {
	case chan *ccxt.WatchPrice:
		ss.removeTickerSubscription(ch)
		close(ch)
	case chan *ccxt.WatchOrderBook:
		ss.removeDepthSubscription(ch)
		close(ch)
		// ... 其他类型
	}

	delete(ss.subscriptionMap, subscriptionID)
	return nil
}

func (ss *StreamSubscribers) removeTickerSubscription(targetCh chan *ccxt.WatchPrice) {
	for i, ch := range ss.tickerSubs {
		if ch == targetCh {
			ss.tickerSubs = append(ss.tickerSubs[:i], ss.tickerSubs[i+1:]...)
			return
		}
	}
}

func (ss *StreamSubscribers) removeDepthSubscription(targetCh chan *ccxt.WatchOrderBook) {
	for i, ch := range ss.depthSubs {
		if ch == targetCh {
			ss.depthSubs = append(ss.depthSubs[:i], ss.depthSubs[i+1:]...)
			return
		}
	}
}

// Close 关闭流管理器
func (sm *StreamManager) Close() {
	sm.cancel()

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 关闭所有stream channels
	for _, streamChan := range sm.streamChannels {
		close(streamChan)
	}

	// 关闭所有订阅者channels
	for _, subscribers := range sm.streamSubscribers {
		subscribers.CloseAll()
	}

	sm.streamChannels = make(map[string]chan []byte)
	sm.streamSubscribers = make(map[string]*StreamSubscribers)
}

// CloseAll 关闭所有订阅者（StreamSubscribers的方法）
func (ss *StreamSubscribers) CloseAll() {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	for _, ch := range ss.subscriptionMap {
		switch ch := ch.(type) {
		case chan *ccxt.WatchPrice:
			close(ch)
		case chan *ccxt.WatchOrderBook:
			close(ch)
		case chan *ccxt.WatchTrade:
			close(ch)
		case chan *ccxt.WatchOHLCV:
			close(ch)
		case chan *ccxt.WatchBalance:
			close(ch)
		case chan *ccxt.WatchOrder:
			close(ch)
		}
	}

	ss.subscriptionMap = make(map[string]interface{})
	ss.tickerSubs = make([]chan *ccxt.WatchPrice, 0)
	ss.depthSubs = make([]chan *ccxt.WatchOrderBook, 0)
	ss.tradeSubs = make([]chan *ccxt.WatchTrade, 0)
	ss.klineSubs = make([]chan *ccxt.WatchOHLCV, 0)
	ss.balanceSubs = make([]chan *ccxt.WatchBalance, 0)
	ss.orderSubs = make([]chan *ccxt.WatchOrder, 0)
}

// GetStats 获取统计信息
func (sm *StreamManager) GetStats() map[string]interface{} {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	streamStats := make(map[string]interface{})
	for streamName, subscribers := range sm.streamSubscribers {
		streamStats[streamName] = subscribers.GetStats()
	}

	return map[string]interface{}{
		"total_streams": len(sm.streamChannels),
		"streams":       streamStats,
	}
}

// GetStats 获取单个stream统计（StreamSubscribers的方法）
func (ss *StreamSubscribers) GetStats() map[string]interface{} {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	return map[string]interface{}{
		"symbol":              ss.symbol,
		"data_type":           ss.dataType,
		"timeframe":           ss.timeframe,
		"ticker_subscribers":  len(ss.tickerSubs),
		"depth_subscribers":   len(ss.depthSubs),
		"trade_subscribers":   len(ss.tradeSubs),
		"kline_subscribers":   len(ss.klineSubs),
		"balance_subscribers": len(ss.balanceSubs),
		"order_subscribers":   len(ss.orderSubs),
		"total_subscribers":   len(ss.subscriptionMap),
	}
}
