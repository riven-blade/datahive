package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/riven-blade/datahive/pkg/ccxt"
	"github.com/riven-blade/datahive/pkg/logger"
	"github.com/riven-blade/datahive/pkg/utils"

	"github.com/spf13/cast"
	"go.uber.org/zap"
)

// ========== Binance WebSocket 实现 ==========

// BinanceWebSocket Binance WebSocket客户端
type BinanceWebSocket struct {
	*ccxt.WebSocketManager
	exchange      *Binance
	isConnected   bool
	subscriptions map[string]bool
	listenKey     string // 私有数据监听密钥
	connection    *ccxt.WebSocketConnection

	// 流管理器 - 负责stream级别的数据分发
	streamManager *StreamManager

	// 简单性能统计
	msgCount    int64
	errorCount  int64
	lastMsgTime int64
}

// NewBinanceWebSocket 创建Binance WebSocket客户端
func NewBinanceWebSocket(exchange *Binance) *BinanceWebSocket {
	return &BinanceWebSocket{
		WebSocketManager: ccxt.NewWebSocketManager(),
		exchange:         exchange,
		subscriptions:    make(map[string]bool),
		streamManager:    NewStreamManager(),
		lastMsgTime:      time.Now().UnixMilli(),
	}
}

// ========== WebSocket 连接管理 ==========

// Connect 连接到Binance WebSocket
func (ws *BinanceWebSocket) Connect(ctx context.Context) error {
	wsURL := ws.getWebSocketURL()
	fmt.Printf("🔌 Connecting to Binance WebSocket: %s\n", wsURL)

	conn, err := ws.WebSocketManager.ConnectWithRetry(ctx, wsURL, "binance-main", ws.exchange.config.WSMaxReconnect)
	if err != nil {
		fmt.Printf("❌ Failed to connect to Binance WebSocket: %v\n", err)
		return err
	}

	ws.connection = conn
	// 设置消息处理器
	ws.setupMessageHandlers(conn)
	ws.isConnected = true

	fmt.Printf("✅ Connected to Binance WebSocket successfully\n")
	return nil
}

// ConnectUserDataStream 连接用户数据流 (需要认证)
func (ws *BinanceWebSocket) ConnectUserDataStream(ctx context.Context) error {
	if !ws.exchange.config.RequiresAuth() {
		return ccxt.NewAuthenticationError("API credentials required for user data stream")
	}

	// 获取监听密钥
	listenKey, err := ws.createListenKey(ctx)
	if err != nil {
		return fmt.Errorf("failed to create listen key: %w", err)
	}
	ws.listenKey = listenKey

	userStreamURL := ws.getUserStreamURL()
	userConn, err := ws.WebSocketManager.ConnectWithRetry(ctx, userStreamURL, "binance-user", ws.exchange.config.WSMaxReconnect)
	if err != nil {
		return err
	}

	// 设置用户数据消息处理器
	ws.setupUserDataHandlers(userConn)

	// 启动监听密钥保活
	go ws.keepListenKeyAlive(ctx)

	return nil
}

// Disconnect 断开WebSocket连接
func (ws *BinanceWebSocket) Disconnect() error {
	ws.isConnected = false

	// 关闭用户数据流
	if ws.listenKey != "" {
		ctx := context.Background()
		err := ws.deleteListenKey(ctx, ws.listenKey)
		if err != nil {
			ws.listenKey = ""
			return err
		}
	}

	// 关闭流管理器
	if ws.streamManager != nil {
		ws.streamManager.Close()
	}

	return nil
}

// IsConnected 检查连接状态
func (ws *BinanceWebSocket) IsConnected() bool {
	return ws.isConnected
}

// getWebSocketURL 获取WebSocket URL
func (ws *BinanceWebSocket) getWebSocketURL() string {
	// 测试网络
	if ws.exchange.config.TestNet {
		if ws.exchange.marketType == "futures" {
			return "wss://fstream.binancefuture.com/ws"
		}
		return "wss://testnet.binance.vision/ws"
	}

	// 生产环境
	switch ws.exchange.marketType {
	case "futures":
		return "wss://fstream.binance.com/ws" // USDM期货
	case "delivery":
		return "wss://dstream.binance.com/ws" // COINM期货
	case "spot":
		return "wss://stream.binance.com:9443/ws" // 现货市场
	default:
		// 其他
		return ""
	}
}

// getUserStreamURL 获取用户数据流URL
func (ws *BinanceWebSocket) getUserStreamURL() string {
	// 测试网络
	if ws.exchange.config.TestNet {
		if ws.exchange.marketType == "futures" {
			return fmt.Sprintf("wss://fstream.binancefuture.com/ws/%s", ws.listenKey)
		}
		return fmt.Sprintf("wss://testnet.binance.vision/ws/%s", ws.listenKey)
	}

	// 生产环境
	switch ws.exchange.marketType {
	case "futures":
		return fmt.Sprintf("wss://fstream.binance.com/ws/%s", ws.listenKey)
	case "delivery":
		return fmt.Sprintf("wss://dstream.binance.com/ws/%s", ws.listenKey)
	default: // spot或其他
		return fmt.Sprintf("wss://stream.binance.com:9443/ws/%s", ws.listenKey)
	}
}

// ========== 监听密钥管理 ==========

// createListenKey 创建监听密钥
func (ws *BinanceWebSocket) createListenKey(ctx context.Context) (string, error) {
	url := "/api/v3/userDataStream"
	params := make(map[string]interface{})

	signedURL, headers, body, err := ws.exchange.Sign(url, "private", "POST", params, nil, nil)
	if err != nil {
		return "", err
	}

	respBody, err := ws.exchange.Fetch(ctx, signedURL, "POST", headers, fmt.Sprintf("%v", body))
	if err != nil {
		return "", err
	}

	var response struct {
		ListenKey string `json:"listenKey"`
	}
	if err := json.Unmarshal([]byte(respBody), &response); err != nil {
		return "", err
	}

	return response.ListenKey, nil
}

// keepListenKeyAlive 保持监听密钥活跃
func (ws *BinanceWebSocket) keepListenKeyAlive(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Minute) // 每30分钟更新一次
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := ws.extendListenKey(ctx, ws.listenKey); err != nil {
				fmt.Printf("Failed to extend listen key: %v\n", err)
			}
		}
	}
}

// extendListenKey 延长监听密钥
func (ws *BinanceWebSocket) extendListenKey(ctx context.Context, listenKey string) error {
	url := "/api/v3/userDataStream"
	params := map[string]interface{}{
		"listenKey": listenKey,
	}

	signedURL, headers, body, err := ws.exchange.Sign(url, "private", "PUT", params, nil, nil)
	if err != nil {
		return err
	}

	_, err = ws.exchange.Fetch(ctx, signedURL, "PUT", headers, fmt.Sprintf("%v", body))
	return err
}

// deleteListenKey 删除监听密钥
func (ws *BinanceWebSocket) deleteListenKey(ctx context.Context, listenKey string) error {
	url := "/api/v3/userDataStream"
	params := map[string]interface{}{
		"listenKey": listenKey,
	}

	signedURL, headers, body, err := ws.exchange.Sign(url, "private", "DELETE", params, nil, nil)
	if err != nil {
		return err
	}

	_, err = ws.exchange.Fetch(ctx, signedURL, "DELETE", headers, fmt.Sprintf("%v", body))
	return err
}

// ========== 消息处理 ==========
func (ws *BinanceWebSocket) setupMessageHandlers(conn *ccxt.WebSocketConnection) {
	conn.SetSubscribeHandler("all", func(data []byte) error {
		return ws.handleMessage(data)
	})
}

// setupUserDataHandlers 设置用户数据处理器
func (ws *BinanceWebSocket) setupUserDataHandlers(conn *ccxt.WebSocketConnection) {
	conn.SetSubscribeHandler("all", func(data []byte) error {
		return ws.handleUserDataMessage(data)
	})
}

// handleUserDataMessage 处理用户数据消息
func (ws *BinanceWebSocket) handleUserDataMessage(data []byte) error {
	var msg map[string]interface{}
	if err := json.Unmarshal(data, &msg); err != nil {
		return err
	}

	eventType, ok := msg["e"].(string)
	if !ok {
		return nil
	}

	// 用户数据流事件处理 - 集成到StreamManager
	switch eventType {
	case "outboundAccountPosition", "balanceUpdate":
		// 余额更新事件 - 路由到StreamManager
		if ws.streamManager != nil {
			return ws.streamManager.RouteMessage(data)
		}
		logger.Debug("StreamManager not available for balance event", zap.String("eventType", eventType))
		return nil
	case "executionReport":
		// 订单执行报告 - 路由到StreamManager
		if ws.streamManager != nil {
			return ws.streamManager.RouteMessage(data)
		}
		logger.Debug("StreamManager not available for order event", zap.String("eventType", eventType))
		return nil
	}

	return nil
}

// SubscribeToStreams 订阅多个数据流
func (ws *BinanceWebSocket) SubscribeToStreams(streams []string) error {
	if !ws.isConnected || ws.connection == nil {
		return fmt.Errorf("websocket not connected")
	}

	// Binance 支持通过单个连接订阅多个流
	subscribeMsg := map[string]interface{}{
		"method": "SUBSCRIBE",
		"params": streams,
		"id":     time.Now().UnixNano(),
	}

	fmt.Printf("📤 Sending subscription message: %+v\n", subscribeMsg)
	return ws.connection.SendMessage(subscribeMsg)
}

// UnsubscribeFromStreams 取消订阅数据流
func (ws *BinanceWebSocket) UnsubscribeFromStreams(streams []string) error {
	if !ws.isConnected || ws.connection == nil {
		return fmt.Errorf("websocket not connected")
	}

	unsubscribeMsg := map[string]interface{}{
		"method": "UNSUBSCRIBE",
		"params": streams,
		"id":     time.Now().UnixNano(),
	}

	return ws.connection.SendMessage(unsubscribeMsg)
}

// handleMessage
func (ws *BinanceWebSocket) handleMessage(data []byte) error {
	// 线程安全地更新统计信息
	atomic.AddInt64(&ws.msgCount, 1)
	atomic.StoreInt64(&ws.lastMsgTime, time.Now().UnixMilli())

	fmt.Printf("📨 Received WebSocket message: %s\n", string(data))

	var msg map[string]interface{}
	if err := json.Unmarshal(data, &msg); err != nil {
		atomic.AddInt64(&ws.errorCount, 1)
		return fmt.Errorf("json parse error: %w", err)
	}

	if _, hasResult := msg["result"]; hasResult {
		fmt.Printf("✅ Subscription confirmation received: %s\n", string(data))
		return nil // 订阅确认消息，忽略
	}

	if errorMsg, ok := msg["error"]; ok {
		atomic.AddInt64(&ws.errorCount, 1)
		fmt.Printf("❌ WebSocket error: %v\n", errorMsg)
		return fmt.Errorf("websocket error: %v", errorMsg)
	}

	// 路由到流管理器进行分发
	if ws.streamManager != nil {
		fmt.Printf("📡 Routing message to stream manager\n")
		return ws.streamManager.RouteMessage(data)
	}

	return nil
}

func (ws *BinanceWebSocket) WatchPrice(ctx context.Context, symbol string, params map[string]interface{}) (string, <-chan *ccxt.WatchPrice, error) {
	streamName := cast.ToString(params["stream_name"])

	subscriptionID, userChan, err := ws.streamManager.SubscribeToStream(streamName, "ticker")
	if err != nil {
		return "", nil, err
	}

	if err := ws.subscribe(streamName); err != nil {
		ws.streamManager.Unsubscribe(subscriptionID)
		return "", nil, err
	}

	priceChan := userChan.(chan *ccxt.WatchPrice)
	return subscriptionID, (<-chan *ccxt.WatchPrice)(priceChan), nil
}

// WatchOrderBook 订阅订单簿数据 - 返回专用channel和订阅ID
func (ws *BinanceWebSocket) WatchOrderBook(ctx context.Context, symbol string, params map[string]interface{}) (string, <-chan *ccxt.WatchOrderBook, error) {
	streamName := cast.ToString(params["stream_name"])

	subscriptionID, userChan, err := ws.streamManager.SubscribeToStream(streamName, "depth")
	if err != nil {
		return "", nil, err
	}

	if err := ws.subscribe(streamName); err != nil {
		ws.streamManager.Unsubscribe(subscriptionID)
		return "", nil, err
	}

	// 类型转换：interface{} -> chan -> <-chan
	orderBookChan := userChan.(chan *ccxt.WatchOrderBook)
	return subscriptionID, (<-chan *ccxt.WatchOrderBook)(orderBookChan), nil
}

// WatchTrades 订阅交易数据 - 返回专用channel和订阅ID
func (ws *BinanceWebSocket) WatchTrades(ctx context.Context, symbol string, params map[string]interface{}) (string, <-chan *ccxt.WatchTrade, error) {
	streamName := cast.ToString(params["stream_name"])

	subscriptionID, userChan, err := ws.streamManager.SubscribeToStream(streamName, "trade")
	if err != nil {
		return "", nil, err
	}

	if err := ws.subscribe(streamName); err != nil {
		ws.streamManager.Unsubscribe(subscriptionID)
		return "", nil, err
	}

	// 类型转换：interface{} -> chan -> <-chan
	tradeChan := userChan.(chan *ccxt.WatchTrade)
	return subscriptionID, (<-chan *ccxt.WatchTrade)(tradeChan), nil
}

// WatchOHLCV 订阅K线数据 - 返回专用channel和订阅ID
func (ws *BinanceWebSocket) WatchOHLCV(ctx context.Context, symbol, timeframe string, params map[string]interface{}) (string, <-chan *ccxt.WatchOHLCV, error) {
	streamName := cast.ToString(params["stream_name"])

	subscriptionID, userChan, err := ws.streamManager.SubscribeToStream(streamName, "kline")
	if err != nil {
		return "", nil, err
	}

	if err := ws.subscribe(streamName); err != nil {
		ws.streamManager.Unsubscribe(subscriptionID)
		return "", nil, err
	}

	// 类型转换：interface{} -> chan -> <-chan
	ohlcvChan := userChan.(chan *ccxt.WatchOHLCV)
	return subscriptionID, (<-chan *ccxt.WatchOHLCV)(ohlcvChan), nil
}

// WatchBalance 订阅账户余额变化 - 返回专用channel和订阅ID
func (ws *BinanceWebSocket) WatchBalance(ctx context.Context, params map[string]interface{}) (string, <-chan *ccxt.WatchBalance, error) {
	if err := ws.ConnectUserDataStream(ctx); err != nil {
		return "", nil, err
	}

	streamName := cast.ToString(params["stream_name"])
	subscriptionID, userChan, err := ws.streamManager.SubscribeToStream(streamName, "balance")
	if err != nil {
		return "", nil, err
	}

	// 类型转换：interface{} -> chan -> <-chan
	balanceChan := userChan.(chan *ccxt.WatchBalance)
	return subscriptionID, (<-chan *ccxt.WatchBalance)(balanceChan), nil
}

// WatchOrders 订阅订单状态变化 - 返回专用channel和订阅ID
func (ws *BinanceWebSocket) WatchOrders(ctx context.Context, params map[string]interface{}) (string, <-chan *ccxt.WatchOrder, error) {
	if err := ws.ConnectUserDataStream(ctx); err != nil {
		return "", nil, err
	}

	streamName := cast.ToString(params["stream_name"])
	subscriptionID, userChan, err := ws.streamManager.SubscribeToStream(streamName, "orders")
	if err != nil {
		return "", nil, err
	}

	// 类型转换：interface{} -> chan -> <-chan
	orderChan := userChan.(chan *ccxt.WatchOrder)
	return subscriptionID, (<-chan *ccxt.WatchOrder)(orderChan), nil
}

// Unsubscribe 取消订阅
func (ws *BinanceWebSocket) Unsubscribe(subscriptionID string) error {
	if ws.streamManager != nil {
		return ws.streamManager.Unsubscribe(subscriptionID)
	}
	return fmt.Errorf("stream manager not available")
}

// WatchMyTrades 订阅我的交易记录
func (ws *BinanceWebSocket) WatchMyTrades(ctx context.Context, symbol string, since int64, limit int, params map[string]interface{}) (<-chan []ccxt.WatchTrade, error) {
	if err := ws.ConnectUserDataStream(ctx); err != nil {
		return nil, err
	}

	// 创建专门的交易频道
	myTradesChan := make(chan []ccxt.WatchTrade, 100)
	return myTradesChan, nil
}

// ========== 订阅管理改进 ==========

// subscribe
func (ws *BinanceWebSocket) subscribe(channel string) error {
	if ws.subscriptions[channel] {
		return nil // 已经订阅
	}

	if !ws.isConnected || ws.connection == nil {
		return fmt.Errorf("websocket not connected")
	}

	// 发送订阅消息
	if err := ws.SubscribeToStreams([]string{channel}); err != nil {
		return err
	}

	ws.subscriptions[channel] = true

	return nil
}

// unsubscribe 取消订阅频道
func (ws *BinanceWebSocket) unsubscribe(channel string) error {
	if !ws.subscriptions[channel] {
		return nil // 未订阅
	}

	if !ws.isConnected || ws.connection == nil {
		return fmt.Errorf("websocket not connected")
	}

	// 发送取消订阅消息
	if err := ws.UnsubscribeFromStreams([]string{channel}); err != nil {
		return err
	}

	delete(ws.subscriptions, channel)

	return nil
}

// convertTimeframe 转换时间帧格式
func (ws *BinanceWebSocket) convertTimeframe(timeframe string) string {
	timeframes := map[string]string{
		"1m":  "1m",
		"3m":  "3m",
		"5m":  "5m",
		"15m": "15m",
		"30m": "30m",
		"1h":  "1h",
		"2h":  "2h",
		"4h":  "4h",
		"6h":  "6h",
		"8h":  "8h",
		"12h": "12h",
		"1d":  "1d",
		"3d":  "3d",
		"1w":  "1w",
		"1M":  "1M",
	}

	if binanceTimeframe, exists := timeframes[timeframe]; exists {
		return binanceTimeframe
	}
	return "1m" // 默认1分钟
}

func (ws *BinanceWebSocket) parsePriceLevels(data []interface{}) []ccxt.PriceLevel {
	levels := make([]ccxt.PriceLevel, 0, len(data))
	for _, levelData := range data {
		if level, ok := levelData.([]interface{}); ok && len(level) >= 2 {
			price := utils.SafeGetFloatWithDefault(map[string]interface{}{"price": level[0]}, "price", 0)
			amount := utils.SafeGetFloatWithDefault(map[string]interface{}{"amount": level[1]}, "amount", 0)
			levels = append(levels, ccxt.PriceLevel{Price: price, Amount: amount})
		}
	}
	return levels
}

// convertToOrderBookSide 转换为OrderBookSide格式
func (ws *BinanceWebSocket) convertToOrderBookSide(levels []ccxt.PriceLevel) ccxt.OrderBookSide {
	prices := make([]float64, len(levels))
	sizes := make([]float64, len(levels))
	for i, level := range levels {
		prices[i] = level.Price
		sizes[i] = level.Amount
	}
	return ccxt.OrderBookSide{Price: prices, Size: sizes}
}

// GetStats 获取简单的性能统计（线程安全）
func (ws *BinanceWebSocket) GetStats() map[string]interface{} {
	msgCount := atomic.LoadInt64(&ws.msgCount)
	errorCount := atomic.LoadInt64(&ws.errorCount)
	lastMsgTime := atomic.LoadInt64(&ws.lastMsgTime)

	var errorRate float64
	if msgCount > 0 {
		errorRate = float64(errorCount) / float64(msgCount) * 100
	}

	return map[string]interface{}{
		"messages_received": msgCount,
		"errors_count":      errorCount,
		"last_message_time": lastMsgTime,
		"error_rate":        errorRate,
		"is_connected":      ws.isConnected,
	}
}

// ResetStats 重置统计信息（线程安全）
func (ws *BinanceWebSocket) ResetStats() {
	atomic.StoreInt64(&ws.msgCount, 0)
	atomic.StoreInt64(&ws.errorCount, 0)
	atomic.StoreInt64(&ws.lastMsgTime, time.Now().UnixMilli())
}

// ========== 辅助方法 ==========

// ISO8601 将时间戳转换为ISO8601格式
func (ws *BinanceWebSocket) ISO8601(timestamp int64) string {
	return time.Unix(timestamp/1000, (timestamp%1000)*1000000).UTC().Format(time.RFC3339)
}

// ========== 连接健康状态管理 ==========

// GetConnectionHealth 获取连接健康状态
func (ws *BinanceWebSocket) GetConnectionHealth() map[string]interface{} {
	generalStats := ws.GetStats()

	health := map[string]interface{}{
		"is_connected":         ws.isConnected,
		"active_subscriptions": len(ws.subscriptions),
		"performance_stats":    generalStats,
		"market_type":          ws.exchange.marketType,
	}

	return health
}
