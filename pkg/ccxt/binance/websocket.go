package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"datahive/pkg/ccxt"
	"sync"
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

	// 数据频道
	tickerChan    chan *ccxt.WatchTicker
	orderBookChan chan *ccxt.WatchOrderBook
	tradesChan    chan []ccxt.WatchTrade
	klinesChan    chan []ccxt.WatchOHLCV
	balanceChan   chan *ccxt.WatchBalance
	ordersChan    chan []ccxt.WatchOrder

	// 频道管理
	channels     map[string]chan interface{}
	channelMutex sync.RWMutex
}

// NewBinanceWebSocket 创建Binance WebSocket客户端
func NewBinanceWebSocket(exchange *Binance) *BinanceWebSocket {
	return &BinanceWebSocket{
		WebSocketManager: ccxt.NewWebSocketManager(),
		exchange:         exchange,
		subscriptions:    make(map[string]bool),
		channels:         make(map[string]chan interface{}),
		tickerChan:       make(chan *ccxt.WatchTicker, 1000),
		orderBookChan:    make(chan *ccxt.WatchOrderBook, 1000),
		tradesChan:       make(chan []ccxt.WatchTrade, 1000),
		klinesChan:       make(chan []ccxt.WatchOHLCV, 1000),
		balanceChan:      make(chan *ccxt.WatchBalance, 1000),
		ordersChan:       make(chan []ccxt.WatchOrder, 1000),
	}
}

// ========== WebSocket 连接管理 ==========

// Connect 连接到Binance WebSocket
func (ws *BinanceWebSocket) Connect(ctx context.Context) error {
	wsURL := ws.getWebSocketURL()
	conn, err := ws.WebSocketManager.ConnectWithRetry(ctx, wsURL, "binance-main", ws.exchange.config.WSMaxReconnect)
	if err != nil {
		return err
	}

	ws.connection = conn
	// 设置消息处理器
	ws.setupMessageHandlers(conn)
	ws.isConnected = true

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

	// 连接用户数据流（带重连）
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
		ws.deleteListenKey(ctx, ws.listenKey)
		ws.listenKey = ""
	}

	// 关闭所有频道
	ws.channelMutex.Lock()
	for _, ch := range ws.channels {
		if ch != nil {
			close(ch)
		}
	}
	ws.channels = make(map[string]chan interface{})
	ws.channelMutex.Unlock()

	close(ws.tickerChan)
	close(ws.orderBookChan)
	close(ws.tradesChan)
	close(ws.klinesChan)
	close(ws.balanceChan)
	close(ws.ordersChan)

	return nil
}

// IsConnected 检查连接状态
func (ws *BinanceWebSocket) IsConnected() bool {
	return ws.isConnected
}

// getWebSocketURL 获取WebSocket URL
func (ws *BinanceWebSocket) getWebSocketURL() string {
	if ws.exchange.config.TestNet {
		return "wss://testnet.binance.vision/ws"
	}
	return "wss://stream.binance.com:9443/ws"
}

// getUserStreamURL 获取用户数据流URL
func (ws *BinanceWebSocket) getUserStreamURL() string {
	if ws.exchange.config.TestNet {
		return fmt.Sprintf("wss://testnet.binance.vision/ws/%s", ws.listenKey)
	}
	return fmt.Sprintf("wss://stream.binance.com:9443/ws/%s", ws.listenKey)
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

// setupMessageHandlers 设置消息处理器
func (ws *BinanceWebSocket) setupMessageHandlers(conn *ccxt.WebSocketConnection) {
	// 处理所有WebSocket消息
	conn.Subscribe("all", func(data []byte) error {
		return ws.handleMessage(data)
	})
}

// setupUserDataHandlers 设置用户数据处理器
func (ws *BinanceWebSocket) setupUserDataHandlers(conn *ccxt.WebSocketConnection) {
	conn.Subscribe("all", func(data []byte) error {
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

	switch eventType {
	case "outboundAccountPosition":
		return ws.handleAccountUpdate(msg)
	case "balanceUpdate":
		return ws.handleBalanceUpdate(msg)
	case "executionReport":
		return ws.handleOrderUpdate(msg)
	}

	return nil
}

// ========== 真正的订阅方法 ==========

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

// ========== 改进的订阅管理 ==========
// (注意：避免重复定义，这些方法在文件前面已经定义)

// ========== 改进的数据处理 ==========

// handleMessage 处理WebSocket消息
func (ws *BinanceWebSocket) handleMessage(data []byte) error {
	var msg map[string]interface{}
	if err := json.Unmarshal(data, &msg); err != nil {
		return err
	}

	// 检查是否是订阅响应
	if result, ok := msg["result"]; ok {
		fmt.Printf("Subscription result: %v\n", result)
		return nil
	}

	// 检查是否是错误消息
	if errorMsg, ok := msg["error"]; ok {
		return fmt.Errorf("websocket error: %v", errorMsg)
	}

	// 检查消息类型
	if stream, ok := msg["stream"].(string); ok {
		return ws.handleStreamMessage(stream, msg["data"])
	}

	// 处理单个流消息
	if eventType, ok := msg["e"].(string); ok {
		return ws.handleEventMessage(eventType, msg)
	}

	return nil
}

// handleStreamMessage 处理流消息
func (ws *BinanceWebSocket) handleStreamMessage(stream string, data interface{}) error {
	parts := strings.Split(stream, "@")
	if len(parts) < 2 {
		return nil
	}

	symbol := strings.ToUpper(parts[0])
	channel := parts[1]

	switch {
	case strings.HasSuffix(channel, "ticker"):
		return ws.handleTickerUpdate(symbol, data)
	case strings.HasSuffix(channel, "depth"):
		return ws.handleDepthUpdate(symbol, data)
	case strings.HasSuffix(channel, "trade"):
		return ws.handleTradeUpdate(symbol, data)
	case strings.HasSuffix(channel, "kline"):
		return ws.handleKlineUpdate(symbol, data)
	}

	return nil
}

// handleEventMessage 处理事件消息
func (ws *BinanceWebSocket) handleEventMessage(eventType string, data interface{}) error {
	switch eventType {
	case "24hrTicker":
		return ws.handleTickerEvent(data)
	case "depthUpdate":
		return ws.handleDepthEvent(data)
	case "trade":
		return ws.handleTradeEvent(data)
	case "kline":
		return ws.handleKlineEvent(data)
	}
	return nil
}

// ========== 数据处理改进 ==========

// handleTickerUpdate 处理Ticker更新
func (ws *BinanceWebSocket) handleTickerUpdate(symbol string, data interface{}) error {
	tickerData, ok := data.(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid ticker data")
	}

	ticker := ws.parseTickerData(symbol, tickerData)
	if ticker != nil {
		// 发送到频道，避免阻塞
		select {
		case ws.tickerChan <- &ccxt.WatchTicker{Ticker: *ticker}:
		default:
			// 频道满时丢弃最旧的数据
		}
	}

	return nil
}

// handleDepthUpdate 处理深度更新
func (ws *BinanceWebSocket) handleDepthUpdate(symbol string, data interface{}) error {
	depthData, ok := data.(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid depth data")
	}

	orderBook := ws.parseDepthData(symbol, depthData)
	if orderBook != nil {
		// 发送到频道，避免阻塞
		select {
		case ws.orderBookChan <- &ccxt.WatchOrderBook{OrderBook: *orderBook}:
		default:
			// 频道满时丢弃最旧的数据
		}
	}

	return nil
}

// handleTradeUpdate 处理交易更新
func (ws *BinanceWebSocket) handleTradeUpdate(symbol string, data interface{}) error {
	tradeData, ok := data.(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid trade data")
	}

	trade := ws.parseTradeData(symbol, tradeData)
	if trade != nil {
		// 发送到频道，避免阻塞
		watchTrade := ccxt.WatchTrade{Trade: *trade}
		select {
		case ws.tradesChan <- []ccxt.WatchTrade{watchTrade}:
		default:
			// 频道满时丢弃最旧的数据
		}
	}

	return nil
}

// handleKlineUpdate 处理K线更新
func (ws *BinanceWebSocket) handleKlineUpdate(symbol string, data interface{}) error {
	klineData, ok := data.(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid kline data")
	}

	kline := ws.parseKlineData(symbol, klineData)
	if kline != nil {
		// 发送到频道，避免阻塞
		watchOHLCV := ccxt.WatchOHLCV{OHLCV: *kline}
		select {
		case ws.klinesChan <- []ccxt.WatchOHLCV{watchOHLCV}:
		default:
			// 频道满时丢弃最旧的数据
		}
	}

	return nil
}

// handleAccountUpdate 处理账户更新
func (ws *BinanceWebSocket) handleAccountUpdate(data map[string]interface{}) error {
	balances := ws.parseAccountBalance(data)
	if balances != nil {
		// 发送到频道，避免阻塞
		select {
		case ws.balanceChan <- &ccxt.WatchBalance{Account: *balances, Channel: "balance"}:
		default:
			// 频道满时丢弃最旧的数据
		}
	}
	return nil
}

// handleBalanceUpdate 处理余额更新
func (ws *BinanceWebSocket) handleBalanceUpdate(data map[string]interface{}) error {
	return ws.handleAccountUpdate(data) // 复用账户更新逻辑
}

// handleOrderUpdate 处理订单更新
func (ws *BinanceWebSocket) handleOrderUpdate(data map[string]interface{}) error {
	order := ws.parseOrderUpdate(data)
	if order != nil {
		// 发送到频道，避免阻塞
		watchOrder := ccxt.WatchOrder{Order: *order, Channel: "orders"}
		select {
		case ws.ordersChan <- []ccxt.WatchOrder{watchOrder}:
		default:
			// 频道满时丢弃最旧的数据
		}
	}
	return nil
}

// ========== 数据解析 ==========

// parseTickerData 解析Ticker数据
func (ws *BinanceWebSocket) parseTickerData(symbol string, data map[string]interface{}) *ccxt.Ticker {
	timestamp := ws.SafeInt(data, "E", ws.getTimestamp())

	return &ccxt.Ticker{
		Symbol:      symbol,
		Timestamp:   timestamp,
		Datetime:    ws.ISO8601(timestamp),
		High:        ws.SafeFloat(data, "h", 0),
		Low:         ws.SafeFloat(data, "l", 0),
		Bid:         ws.SafeFloat(data, "b", 0),
		BidVolume:   ws.SafeFloat(data, "B", 0),
		Ask:         ws.SafeFloat(data, "a", 0),
		AskVolume:   ws.SafeFloat(data, "A", 0),
		Vwap:        ws.SafeFloat(data, "w", 0),
		Open:        ws.SafeFloat(data, "o", 0),
		Close:       ws.SafeFloat(data, "c", 0),
		Last:        ws.SafeFloat(data, "c", 0),
		Change:      ws.SafeFloat(data, "p", 0),
		Percentage:  ws.SafeFloat(data, "P", 0),
		BaseVolume:  ws.SafeFloat(data, "v", 0),
		QuoteVolume: ws.SafeFloat(data, "q", 0),
		Info:        data,
	}
}

// parseDepthData 解析深度数据
func (ws *BinanceWebSocket) parseDepthData(symbol string, data map[string]interface{}) *ccxt.OrderBook {
	timestamp := ws.SafeInt(data, "E", ws.getTimestamp())

	// 解析买单和卖单
	var bids []ccxt.PriceLevel
	var asks []ccxt.PriceLevel

	if bidsData, ok := data["b"].([]interface{}); ok {
		for _, bidData := range bidsData {
			if bid, ok := bidData.([]interface{}); ok && len(bid) >= 2 {
				price := ws.SafeFloat(map[string]interface{}{"price": bid[0]}, "price", 0)
				amount := ws.SafeFloat(map[string]interface{}{"amount": bid[1]}, "amount", 0)
				bids = append(bids, ccxt.PriceLevel{Price: price, Amount: amount})
			}
		}
	}

	if asksData, ok := data["a"].([]interface{}); ok {
		for _, askData := range asksData {
			if ask, ok := askData.([]interface{}); ok && len(ask) >= 2 {
				price := ws.SafeFloat(map[string]interface{}{"price": ask[0]}, "price", 0)
				amount := ws.SafeFloat(map[string]interface{}{"amount": ask[1]}, "amount", 0)
				asks = append(asks, ccxt.PriceLevel{Price: price, Amount: amount})
			}
		}
	}

	return &ccxt.OrderBook{
		Symbol:    symbol,
		Bids:      bids,
		Asks:      asks,
		Timestamp: timestamp,
		Datetime:  ws.ISO8601(timestamp),
		Info:      data,
	}
}

// parseTradeData 解析交易数据
func (ws *BinanceWebSocket) parseTradeData(symbol string, data map[string]interface{}) *ccxt.Trade {
	timestamp := ws.SafeInt(data, "T", ws.getTimestamp())
	price := ws.SafeFloat(data, "p", 0)
	amount := ws.SafeFloat(data, "q", 0)

	var side string
	if ws.SafeBool(data, "m", false) {
		side = "sell" // 主动卖出单
	} else {
		side = "buy" // 主动买入单
	}

	return &ccxt.Trade{
		ID:        fmt.Sprintf("%v", ws.SafeInt(data, "t", 0)),
		Symbol:    symbol,
		Timestamp: timestamp,
		Datetime:  ws.ISO8601(timestamp),
		Side:      side,
		Amount:    amount,
		Price:     price,
		Cost:      price * amount,
		Info:      data,
	}
}

// parseKlineData 解析K线数据
func (ws *BinanceWebSocket) parseKlineData(symbol string, data map[string]interface{}) *ccxt.OHLCV {
	if k, ok := data["k"].(map[string]interface{}); ok {
		return ws.parseKlineEventData(symbol, k)
	}
	return nil
}

// parseKlineEventData 解析K线事件数据
func (ws *BinanceWebSocket) parseKlineEventData(symbol string, k map[string]interface{}) *ccxt.OHLCV {
	timestamp := ws.SafeInt(k, "t", 0)

	return &ccxt.OHLCV{
		Timestamp: timestamp,
		Open:      ws.SafeFloat(k, "o", 0),
		High:      ws.SafeFloat(k, "h", 0),
		Low:       ws.SafeFloat(k, "l", 0),
		Close:     ws.SafeFloat(k, "c", 0),
		Volume:    ws.SafeFloat(k, "v", 0),
	}
}

// ========== 订阅管理 ==========

// WatchTicker 订阅Ticker数据
func (ws *BinanceWebSocket) WatchTicker(ctx context.Context, symbol string, params map[string]interface{}) (<-chan *ccxt.WatchTicker, error) {
	channel := fmt.Sprintf("%s@ticker", strings.ToLower(ws.convertSymbol(symbol)))

	if err := ws.subscribe(channel); err != nil {
		return nil, err
	}

	return ws.tickerChan, nil
}

// WatchOrderBook 订阅订单簿数据
func (ws *BinanceWebSocket) WatchOrderBook(ctx context.Context, symbol string, limit int, params map[string]interface{}) (<-chan *ccxt.WatchOrderBook, error) {
	var channel string
	if limit > 0 {
		channel = fmt.Sprintf("%s@depth%d", strings.ToLower(ws.convertSymbol(symbol)), limit)
	} else {
		channel = fmt.Sprintf("%s@depth", strings.ToLower(ws.convertSymbol(symbol)))
	}

	if err := ws.subscribe(channel); err != nil {
		return nil, err
	}

	return ws.orderBookChan, nil
}

// WatchTrades 订阅交易数据
func (ws *BinanceWebSocket) WatchTrades(ctx context.Context, symbol string, since int64, limit int, params map[string]interface{}) (<-chan []ccxt.WatchTrade, error) {
	channel := fmt.Sprintf("%s@trade", strings.ToLower(ws.convertSymbol(symbol)))

	if err := ws.subscribe(channel); err != nil {
		return nil, err
	}

	return ws.tradesChan, nil
}

// WatchOHLCV 订阅K线数据
func (ws *BinanceWebSocket) WatchOHLCV(ctx context.Context, symbol, timeframe string, since int64, limit int, params map[string]interface{}) (<-chan []ccxt.WatchOHLCV, error) {
	interval := ws.convertTimeframe(timeframe)
	channel := fmt.Sprintf("%s@kline_%s", strings.ToLower(ws.convertSymbol(symbol)), interval)

	if err := ws.subscribe(channel); err != nil {
		return nil, err
	}

	return ws.klinesChan, nil
}

// ========== 用户数据流订阅 ==========

// WatchBalance 订阅账户余额变化
func (ws *BinanceWebSocket) WatchBalance(ctx context.Context, params map[string]interface{}) (<-chan *ccxt.WatchBalance, error) {
	if err := ws.ConnectUserDataStream(ctx); err != nil {
		return nil, err
	}

	return ws.balanceChan, nil
}

// WatchOrders 订阅订单状态变化
func (ws *BinanceWebSocket) WatchOrders(ctx context.Context, symbol string, since int64, limit int, params map[string]interface{}) (<-chan []ccxt.WatchOrder, error) {
	if err := ws.ConnectUserDataStream(ctx); err != nil {
		return nil, err
	}

	return ws.ordersChan, nil
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

// subscribe 改进版订阅频道
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

// convertSymbol 转换交易对格式 (BTC/USDT -> BTCUSDT)
func (ws *BinanceWebSocket) convertSymbol(symbol string) string {
	return strings.ReplaceAll(symbol, "/", "")
}

// normalizeSymbol 标准化交易对格式 (BTCUSDT -> BTC/USDT)
func (ws *BinanceWebSocket) normalizeSymbol(symbol string) string {
	// 这里需要根据市场信息来正确分割symbol
	// 简化实现，假设都是/USDT结尾
	if strings.HasSuffix(symbol, "USDT") {
		base := symbol[:len(symbol)-4]
		return base + "/USDT"
	}
	return symbol
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

// ========== 辅助方法 ==========

// SafeString 安全获取字符串值
func (ws *BinanceWebSocket) SafeString(data map[string]interface{}, key, defaultValue string) string {
	if value, exists := data[key]; exists {
		if str, ok := value.(string); ok {
			return str
		}
	}
	return defaultValue
}

// SafeFloat 安全获取浮点数值
func (ws *BinanceWebSocket) SafeFloat(data map[string]interface{}, key string, defaultValue float64) float64 {
	if value, exists := data[key]; exists {
		switch v := value.(type) {
		case float64:
			return v
		case string:
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return f
			}
		case int:
			return float64(v)
		case int64:
			return float64(v)
		}
	}
	return defaultValue
}

// SafeInt 安全获取整数值
func (ws *BinanceWebSocket) SafeInt(data map[string]interface{}, key string, defaultValue int64) int64 {
	if value, exists := data[key]; exists {
		switch v := value.(type) {
		case int64:
			return v
		case int:
			return int64(v)
		case float64:
			return int64(v)
		case string:
			if i, err := strconv.ParseInt(v, 10, 64); err == nil {
				return i
			}
		}
	}
	return defaultValue
}

// SafeBool 安全获取布尔值
func (ws *BinanceWebSocket) SafeBool(data map[string]interface{}, key string, defaultValue bool) bool {
	if value, exists := data[key]; exists {
		if b, ok := value.(bool); ok {
			return b
		}
	}
	return defaultValue
}

// getTimestamp 获取当前时间戳
func (ws *BinanceWebSocket) getTimestamp() int64 {
	return time.Now().UnixMilli()
}

// ISO8601 将时间戳转换为ISO8601格式
func (ws *BinanceWebSocket) ISO8601(timestamp int64) string {
	return time.Unix(timestamp/1000, (timestamp%1000)*1000000).UTC().Format(time.RFC3339)
}

// ========== 用户数据解析 ==========
// handleTickerEvent 处理Ticker事件
func (ws *BinanceWebSocket) handleTickerEvent(data interface{}) error {
	if tickerData, ok := data.(map[string]interface{}); ok {
		symbol := ws.SafeString(tickerData, "s", "")
		return ws.handleTickerUpdate(symbol, tickerData)
	}
	return nil
}

// handleDepthEvent 处理深度事件
func (ws *BinanceWebSocket) handleDepthEvent(data interface{}) error {
	if depthData, ok := data.(map[string]interface{}); ok {
		symbol := ws.SafeString(depthData, "s", "")
		return ws.handleDepthUpdate(symbol, depthData)
	}
	return nil
}

// handleTradeEvent 处理交易事件
func (ws *BinanceWebSocket) handleTradeEvent(data interface{}) error {
	if tradeData, ok := data.(map[string]interface{}); ok {
		symbol := ws.SafeString(tradeData, "s", "")
		return ws.handleTradeUpdate(symbol, tradeData)
	}
	return nil
}

// handleKlineEvent 处理K线事件
func (ws *BinanceWebSocket) handleKlineEvent(data interface{}) error {
	if klineData, ok := data.(map[string]interface{}); ok {
		if k, exists := klineData["k"].(map[string]interface{}); exists {
			symbol := ws.SafeString(k, "s", "")
			return ws.handleKlineUpdate(symbol, klineData)
		}
	}
	return nil
}
