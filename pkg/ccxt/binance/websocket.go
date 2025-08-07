package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/riven-blade/datahive/pkg/ccxt"
	"github.com/riven-blade/datahive/pkg/logger"
	"github.com/riven-blade/datahive/pkg/protocol"
	"github.com/spf13/cast"
	"go.uber.org/zap"
)

// ============================================================================
// Binance WebSocket Client
// ============================================================================

// MessageRateLimiter WebSocket消息频率限制器
// Binance限制: 每秒最多5条消息 (包括PING, PONG, JSON控制消息)
type MessageRateLimiter struct {
	tokens         int32         // 令牌数量
	maxTokens      int32         // 最大令牌数 (5)
	refillInterval time.Duration // 令牌填充间隔 (200ms per token)
	lastRefill     int64         // 上次填充时间 (UnixMilli)
	mu             sync.Mutex    // 互斥锁
}

// NewMessageRateLimiter 创建消息频率限制器
func NewMessageRateLimiter() *MessageRateLimiter {
	return &MessageRateLimiter{
		tokens:         5,                      // 初始满桶
		maxTokens:      5,                      // 每秒5条消息
		refillInterval: 200 * time.Millisecond, // 每200ms补充1个令牌
		lastRefill:     time.Now().UnixMilli(),
	}
}

// Allow 检查是否允许发送消息 (非阻塞)
func (mrl *MessageRateLimiter) Allow() bool {
	mrl.mu.Lock()
	defer mrl.mu.Unlock()

	// 填充令牌
	mrl.refillTokens()

	// 检查是否有令牌
	if mrl.tokens > 0 {
		mrl.tokens--
		return true
	}
	return false
}

// Wait 等待直到可以发送消息 (阻塞)
func (mrl *MessageRateLimiter) Wait(ctx context.Context) error {
	for {
		if mrl.Allow() {
			return nil
		}

		// 等待一小段时间再重试
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
			continue
		}
	}
}

// refillTokens 填充令牌
func (mrl *MessageRateLimiter) refillTokens() {
	now := time.Now().UnixMilli()
	elapsed := now - mrl.lastRefill

	if elapsed >= int64(mrl.refillInterval.Milliseconds()) {
		// 计算应该添加的令牌数
		tokensToAdd := int32(elapsed / int64(mrl.refillInterval.Milliseconds()))
		mrl.tokens = min32(mrl.maxTokens, mrl.tokens+tokensToAdd)
		mrl.lastRefill = now
	}
}

// GetStatus 获取限制器状态
func (mrl *MessageRateLimiter) GetStatus() map[string]interface{} {
	mrl.mu.Lock()
	defer mrl.mu.Unlock()

	mrl.refillTokens()
	return map[string]interface{}{
		"tokens":     mrl.tokens,
		"max_tokens": mrl.maxTokens,
		"rate_limit": "5 messages per second",
	}
}

// 辅助函数
func min32(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

// WebSocketConfig 最佳实践配置
type WebSocketConfig struct {
	// 连接配置
	MaxConnections       int           `json:"max_connections"`        // 最大连接数 (10)
	StreamsPerConnection int           `json:"streams_per_connection"` // 每连接流数 (1000)
	ReconnectDelay       time.Duration `json:"reconnect_delay"`        // 重连延迟 (3s)
	MaxReconnectAttempts int           `json:"max_reconnect_attempts"` // 最大重连次数 (5)

	// 批量处理
	BatchSize     int           `json:"batch_size"`     // 批量大小 (200)
	BatchInterval time.Duration `json:"batch_interval"` // 批量间隔 (100ms)

	// 性能配置
	ChannelBuffer       int           `json:"channel_buffer"`        // 通道缓冲 (1000)
	HealthCheckInterval time.Duration `json:"health_check_interval"` // 健康检查 (30s)

	// 自动清理
	AutoCleanup     bool          `json:"auto_cleanup"`     // 自动清理
	CleanupInterval time.Duration `json:"cleanup_interval"` // 清理间隔 (60s)
	IdleTimeout     time.Duration `json:"idle_timeout"`     // 空闲超时 (5m)
}

// DefaultWebSocketConfig 默认配置
func DefaultWebSocketConfig() *WebSocketConfig {
	return &WebSocketConfig{
		MaxConnections:       10,
		StreamsPerConnection: 1000,
		ReconnectDelay:       3 * time.Second,
		MaxReconnectAttempts: 5,
		BatchSize:            200,
		BatchInterval:        100 * time.Millisecond,
		ChannelBuffer:        1000,
		HealthCheckInterval:  30 * time.Second,
		AutoCleanup:          true,
		CleanupInterval:      60 * time.Second,
		IdleTimeout:          5 * time.Minute,
	}
}

// WebSocket 最佳实践WebSocket客户端
type WebSocket struct {
	config   *WebSocketConfig
	exchange *Binance

	// 连接池
	connections []*WSConnection
	connMutex   sync.RWMutex

	// 流管理
	streams       sync.Map // streamName -> *StreamData
	subscriptions sync.Map // subscriptionID -> *SubData

	// 批量处理
	batchChan chan string
	batchMap  sync.Map // streamName -> bool

	// 消息频率限制器 (Binance限制: 每秒5条消息)
	msgRateLimiter *MessageRateLimiter

	// 状态
	isRunning   int32
	msgCount    int64
	errorCount  int64
	lastMsgTime int64

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// WSConnection WebSocket连接
type WSConnection struct {
	ID          string
	ws          *ccxt.WebSocketConnection
	streamCount int32
	isHealthy   int32
	lastUsed    time.Time
	mu          sync.RWMutex
}

// StreamData 流数据
type StreamData struct {
	Name        string
	DataType    string
	Connection  *WSConnection
	Subscribers sync.Map // subscriptionID -> *SubData
	SubCount    int32
	LastUsed    time.Time
	MsgCount    int64
}

// SubData 订阅数据
type SubData struct {
	ID         string
	StreamName string
	DataType   string
	Channel    interface{}
	CreatedAt  time.Time
	LastUsed   time.Time
}

// BatchItem 批量项
type BatchItem struct {
	StreamName string
	Timestamp  time.Time
}

// NewWebSocket 创建最佳实践WebSocket客户端
func NewWebSocket(exchange *Binance, config *WebSocketConfig) *WebSocket {
	if config == nil {
		config = DefaultWebSocketConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &WebSocket{
		config:         config,
		exchange:       exchange,
		batchChan:      make(chan string, config.BatchSize*2),
		msgRateLimiter: NewMessageRateLimiter(),
		ctx:            ctx,
		cancel:         cancel,
		lastMsgTime:    time.Now().UnixMilli(),
	}
}

// Start 启动WebSocket客户端
func (ws *WebSocket) Start() error {
	if !atomic.CompareAndSwapInt32(&ws.isRunning, 0, 1) {
		return fmt.Errorf("websocket already running")
	}

	// 创建初始连接
	if err := ws.createConnection(); err != nil {
		atomic.StoreInt32(&ws.isRunning, 0)
		return fmt.Errorf("failed to create connection: %w", err)
	}

	// 启动批量处理器
	ws.wg.Add(1)
	go ws.batchProcessor()

	// 启动健康检查
	ws.wg.Add(1)
	go ws.healthChecker()

	// 启动清理器
	if ws.config.AutoCleanup {
		ws.wg.Add(1)
		go ws.cleaner()
	}

	logger.Info("Best WebSocket client started",
		zap.Int("max_connections", ws.config.MaxConnections),
		zap.Int("streams_per_connection", ws.config.StreamsPerConnection))

	return nil
}

// Stop 停止WebSocket客户端
func (ws *WebSocket) Stop() {
	if !atomic.CompareAndSwapInt32(&ws.isRunning, 1, 0) {
		return
	}

	ws.cancel()
	ws.wg.Wait()

	// 关闭连接
	ws.connMutex.Lock()
	for _, conn := range ws.connections {
		ws.closeConnection(conn)
	}
	ws.connections = nil
	ws.connMutex.Unlock()

	// 关闭通道
	ws.subscriptions.Range(func(key, value interface{}) bool {
		if sub, ok := value.(*SubData); ok {
			ws.closeChannel(sub.Channel)
		}
		return true
	})

	logger.Info("Best WebSocket client stopped")
}

// Subscribe 订阅数据流
func (ws *WebSocket) Subscribe(streamName, dataType string) (string, interface{}, error) {
	if atomic.LoadInt32(&ws.isRunning) == 0 {
		return "", nil, fmt.Errorf("websocket not running")
	}

	// 生成订阅ID
	subID := fmt.Sprintf("%s_%d", streamName, time.Now().UnixNano())

	// 创建通道
	channel := ws.createChannel(dataType)
	if channel == nil {
		return "", nil, fmt.Errorf("unsupported data type: %s", dataType)
	}

	// 获取或创建流
	streamData := ws.getOrCreateStream(streamName, dataType)

	// 创建订阅
	subData := &SubData{
		ID:         subID,
		StreamName: streamName,
		DataType:   dataType,
		Channel:    channel,
		CreatedAt:  time.Now(),
		LastUsed:   time.Now(),
	}

	// 注册订阅
	ws.subscriptions.Store(subID, subData)
	streamData.Subscribers.Store(subID, subData)
	atomic.AddInt32(&streamData.SubCount, 1)

	// 添加到批量队列
	ws.addToBatch(streamName)

	logger.Debug("Stream subscribed",
		zap.String("subscription_id", subID),
		zap.String("stream_name", streamName))

	return subID, channel, nil
}

// Unsubscribe 取消订阅
func (ws *WebSocket) Unsubscribe(subscriptionID string) error {
	subInterface, exists := ws.subscriptions.Load(subscriptionID)
	if !exists {
		return fmt.Errorf("subscription not found")
	}

	subData := subInterface.(*SubData)

	// 从流中移除
	if streamInterface, exists := ws.streams.Load(subData.StreamName); exists {
		streamData := streamInterface.(*StreamData)
		streamData.Subscribers.Delete(subscriptionID)
		atomic.AddInt32(&streamData.SubCount, -1)

		// 如果没有订阅者了，发送取消订阅消息
		if atomic.LoadInt32(&streamData.SubCount) == 0 {
			ws.sendUnsubscribeMessage(subData.StreamName)
		}
	}

	// 关闭通道
	ws.closeChannel(subData.Channel)

	// 删除订阅
	ws.subscriptions.Delete(subscriptionID)

	logger.Debug("Stream unsubscribed", zap.String("subscription_id", subscriptionID))
	return nil
}

// sendUnsubscribeMessage 发送取消订阅消息
func (ws *WebSocket) sendUnsubscribeMessage(streamName string) {
	conn := ws.selectBestConnection()
	if conn == nil {
		logger.Error("No connection available for unsubscribe")
		return
	}

	// 构造取消订阅消息
	unsubscribeMsg := map[string]interface{}{
		FieldMethod: MethodUnsubscribe,
		FieldParams: []string{streamName},
		FieldId:     time.Now().UnixNano(),
	}

	// 应用消息频率限制 (Binance: 每秒最多5条消息)
	if err := ws.msgRateLimiter.Wait(ws.ctx); err != nil {
		logger.Warn("Message rate limiter cancelled during unsubscribe", zap.Error(err))
		return
	}

	if err := conn.ws.SendMessage(unsubscribeMsg); err != nil {
		logger.Error("Failed to send unsubscribe", zap.Error(err), zap.String("stream", streamName))
		return
	}

	logger.Debug("Unsubscribe message sent", zap.String("stream", streamName))
}

// ========== 内部方法 ==========

// createConnection 创建连接
func (ws *WebSocket) createConnection() error {
	ws.connMutex.Lock()
	defer ws.connMutex.Unlock()

	if len(ws.connections) >= ws.config.MaxConnections {
		return fmt.Errorf("max connections reached")
	}

	connID := fmt.Sprintf("best_%d_%d", len(ws.connections), time.Now().UnixNano())
	manager := ccxt.NewWebSocketManager()

	wsURL := ws.getWebSocketURL()
	if wsURL == "" {
		return fmt.Errorf("websocket URL not configured")
	}

	wsInst, err := manager.ConnectWithRetry(ws.ctx, wsURL, connID, ws.config.MaxReconnectAttempts)
	if err != nil {
		return err
	}

	conn := &WSConnection{
		ID:        connID,
		ws:        wsInst,
		isHealthy: 1,
		lastUsed:  time.Now(),
	}

	// 设置消息处理器
	wsInst.SetSubscribeHandler(ccxt.HandlerTypeAll, func(data []byte) error {
		return ws.handleMessage(data, conn)
	})

	ws.connections = append(ws.connections, conn)

	logger.Info("WebSocket connection created", zap.String("connection_id", connID))
	return nil
}

// getOrCreateStream 获取或创建流
func (ws *WebSocket) getOrCreateStream(streamName, dataType string) *StreamData {
	if streamInterface, exists := ws.streams.Load(streamName); exists {
		return streamInterface.(*StreamData)
	}

	conn := ws.selectBestConnection()
	if conn == nil {
		ws.createConnection()
		conn = ws.selectBestConnection()
	}

	streamData := &StreamData{
		Name:       streamName,
		DataType:   dataType,
		Connection: conn,
		LastUsed:   time.Now(),
	}

	ws.streams.Store(streamName, streamData)

	if conn != nil {
		atomic.AddInt32(&conn.streamCount, 1)
	}

	return streamData
}

// selectBestConnection 选择最佳连接
func (ws *WebSocket) selectBestConnection() *WSConnection {
	ws.connMutex.RLock()
	defer ws.connMutex.RUnlock()

	var bestConn *WSConnection
	var minLoad int32 = int32(ws.config.StreamsPerConnection)

	for _, conn := range ws.connections {
		if atomic.LoadInt32(&conn.isHealthy) == 0 {
			continue
		}

		load := atomic.LoadInt32(&conn.streamCount)
		if load < minLoad {
			minLoad = load
			bestConn = conn
		}
	}

	return bestConn
}

// addToBatch 添加到批量队列
func (ws *WebSocket) addToBatch(streamName string) {
	// 检查是否已在批量队列中
	if _, exists := ws.batchMap.LoadOrStore(streamName, true); exists {
		return
	}

	select {
	case ws.batchChan <- streamName:
	default:
		logger.Warn("Batch channel full", zap.String("stream", streamName))
	}
}

// batchProcessor 批量处理器
func (ws *WebSocket) batchProcessor() {
	defer ws.wg.Done()

	batch := make([]string, 0, ws.config.BatchSize)
	ticker := time.NewTicker(ws.config.BatchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ws.ctx.Done():
			if len(batch) > 0 {
				ws.processBatch(batch)
			}
			return

		case stream := <-ws.batchChan:
			batch = append(batch, stream)
			if len(batch) >= ws.config.BatchSize {
				ws.processBatch(batch)
				batch = batch[:0]
			}

		case <-ticker.C:
			if len(batch) > 0 {
				ws.processBatch(batch)
				batch = batch[:0]
			}
		}
	}
}

// processBatch 处理批量
func (ws *WebSocket) processBatch(streams []string) {
	if len(streams) == 0 {
		return
	}

	conn := ws.selectBestConnection()
	if conn == nil {
		logger.Error("No connection available for batch")
		return
	}

	// 清除批量映射
	for _, stream := range streams {
		ws.batchMap.Delete(stream)
	}

	// 发送订阅
	subscribeMsg := map[string]interface{}{
		FieldMethod: MethodSubscribe,
		FieldParams: streams,
		FieldId:     time.Now().UnixNano(),
	}

	// 应用消息频率限制 (Binance: 每秒最多5条消息)
	if err := ws.msgRateLimiter.Wait(ws.ctx); err != nil {
		logger.Warn("Message rate limiter cancelled", zap.Error(err))
		// 重新添加到队列
		for _, stream := range streams {
			ws.addToBatch(stream)
		}
		return
	}

	if err := conn.ws.SendMessage(subscribeMsg); err != nil {
		logger.Error("Failed to send batch", zap.Error(err))
		// 重新添加到队列
		for _, stream := range streams {
			ws.addToBatch(stream)
		}
		return
	}

	logger.Debug("Batch sent",
		zap.String("connection", conn.ID),
		zap.Strings("streams", streams))
}

// handleMessage 处理消息
func (ws *WebSocket) handleMessage(data []byte, conn *WSConnection) error {
	atomic.AddInt64(&ws.msgCount, 1)
	atomic.StoreInt64(&ws.lastMsgTime, time.Now().UnixMilli())

	var msg map[string]interface{}
	if err := json.Unmarshal(data, &msg); err != nil {
		atomic.AddInt64(&ws.errorCount, 1)
		return err
	}

	// 处理订阅确认
	if _, hasResult := msg[FieldResult]; hasResult {
		return nil
	}

	// 处理错误
	if errorMsg, ok := msg[FieldError]; ok {
		atomic.AddInt64(&ws.errorCount, 1)
		return fmt.Errorf("websocket error: %v", errorMsg)
	}

	// 解析流名称并分发
	streamName := ws.parseStreamName(msg)
	if streamName != "" {
		return ws.distributeMessage(streamName, msg)
	}

	return nil
}

// parseStreamName 解析流名称
func (ws *WebSocket) parseStreamName(msg map[string]interface{}) string {
	if stream, ok := msg[FieldStream].(string); ok {
		return stream
	}

	if eventType, ok := msg[FieldEventType].(string); ok {
		if symbol, ok := msg[FieldSymbol].(string); ok {
			return ws.constructStreamName(symbol, eventType, msg)
		}
	}

	return ""
}

// constructStreamName 构造流名称
func (ws *WebSocket) constructStreamName(symbol, eventType string, msg map[string]interface{}) string {
	symbol = strings.ToLower(symbol)
	switch eventType {
	case EventTypeTrade:
		return fmt.Sprintf(StreamTemplateTrade, symbol)
	case EventType24hrTicker:
		return fmt.Sprintf(StreamTemplateTicker, symbol)
	case EventType24hrMiniTicker:
		return fmt.Sprintf(StreamTemplateMiniTicker, symbol)
	case EventTypeBookTicker:
		return fmt.Sprintf(StreamTemplateBookTicker, symbol)
	case EventTypeMarkPrice:
		return fmt.Sprintf(StreamTemplateMarkPrice, symbol)
	case EventTypeKline:
		if kData, ok := msg[FieldKlineData].(map[string]interface{}); ok {
			if interval, ok := kData[FieldKlineInterval].(string); ok {
				return fmt.Sprintf(StreamTemplateKline, symbol, interval)
			}
		}
		return fmt.Sprintf(StreamTemplateKlineDefault, symbol)
	case EventTypeDepthUpdate:
		return fmt.Sprintf(StreamTemplateDepth, symbol)
	default:
		return fmt.Sprintf("%s@%s", symbol, eventType)
	}
}

// distributeMessage 分发消息
func (ws *WebSocket) distributeMessage(streamName string, data map[string]interface{}) error {
	streamInterface, exists := ws.streams.Load(streamName)
	if !exists {
		return nil
	}

	streamData := streamInterface.(*StreamData)
	atomic.AddInt64(&streamData.MsgCount, 1)
	streamData.LastUsed = time.Now()

	// 处理多路复用
	if dataField, ok := data[FieldData].(map[string]interface{}); ok {
		data = dataField
	}

	// 解析数据
	parsedData := ws.parseData(data, streamData.DataType, streamData.Name)
	if parsedData == nil {
		return nil
	}

	// 分发到订阅者
	streamData.Subscribers.Range(func(key, value interface{}) bool {
		if sub, ok := value.(*SubData); ok {
			sub.LastUsed = time.Now()
			ws.sendToChannel(sub.Channel, parsedData)
		}
		return true
	})

	return nil
}

// parseData 解析数据
func (ws *WebSocket) parseData(data map[string]interface{}, dataType, streamName string) interface{} {
	switch dataType {
	case protocol.StreamEventMiniTicker:
		return &ccxt.WatchMiniTicker{
			Symbol:      getString(data, FieldSymbol),
			TimeStamp:   getInt64(data, FieldEventTime),
			Open:        getFloat64(data, FieldOpen),
			High:        getFloat64(data, FieldHigh),
			Low:         getFloat64(data, FieldLow),
			Close:       getFloat64(data, FieldClose),
			Volume:      getFloat64(data, FieldVolume),
			QuoteVolume: getFloat64(data, FieldQuoteVolume),
			StreamName:  streamName,
		}
	case protocol.StreamEventMarkPrice:
		return &ccxt.WatchMarkPrice{
			Symbol:      getString(data, FieldSymbol),
			TimeStamp:   getInt64(data, FieldEventTime),
			MarkPrice:   getFloat64(data, FieldMarkPrice),
			IndexPrice:  getFloat64(data, FieldIndexPrice),
			FundingRate: getFloat64(data, FieldFundingRate),
			FundingTime: getInt64(data, FieldFundingTime),
			StreamName:  streamName,
		}
	case protocol.StreamEventBookTicker:
		return &ccxt.WatchBookTicker{
			Symbol:      getString(data, FieldSymbol),
			TimeStamp:   getInt64(data, FieldUpdateId),
			BidPrice:    getFloat64(data, FieldBidPrice),
			BidQuantity: getFloat64(data, FieldBidQty),
			AskPrice:    getFloat64(data, FieldAskPrice),
			AskQuantity: getFloat64(data, FieldAskQty),
			StreamName:  streamName,
		}
	case protocol.StreamEventOrderBook:
		// 解析订单簿数据
		var bids, asks [][]float64
		if bidsData, ok := data["b"].([]interface{}); ok {
			for _, bid := range bidsData {
				if bidArray, ok := bid.([]interface{}); ok && len(bidArray) >= 2 {
					price := getFloat64Interface(bidArray[0])
					quantity := getFloat64Interface(bidArray[1])
					bids = append(bids, []float64{price, quantity})
				}
			}
		}
		if asksData, ok := data["a"].([]interface{}); ok {
			for _, ask := range asksData {
				if askArray, ok := ask.([]interface{}); ok && len(askArray) >= 2 {
					price := getFloat64Interface(askArray[0])
					quantity := getFloat64Interface(askArray[1])
					asks = append(asks, []float64{price, quantity})
				}
			}
		}
		return &ccxt.WatchOrderBook{
			Symbol:     getString(data, FieldSymbol),
			TimeStamp:  getInt64(data, FieldEventTime),
			Bids:       bids,
			Asks:       asks,
			Nonce:      getInt64(data, "u"), // u是updateId，作为nonce使用
			StreamName: streamName,
		}
	case protocol.StreamEventTrade:
		price := getFloat64(data, FieldPrice)
		amount := getFloat64(data, FieldQuantity)
		return &ccxt.WatchTrade{
			ID:        getString(data, FieldTradeId),
			Symbol:    getString(data, FieldSymbol),
			Timestamp: getInt64(data, FieldTradeTime),
			Price:     price,
			Amount:    amount,
			Cost:      price * amount,
			Side: func() string {
				if getBool(data, "m") {
					return "sell"
				} else {
					return "buy"
				}
			}(), // m表示是否是做市商买入
			TakerOrMaker: func() string {
				if getBool(data, "m") {
					return "maker"
				} else {
					return "taker"
				}
			}(),
			Type:        "market", // WebSocket交易事件通常是市价交易
			Fee:         0,        // WebSocket流中通常不包含手续费信息
			FeeCurrency: "",
			StreamName:  streamName,
		}
	case protocol.StreamEventKline:
		if kData, ok := data[FieldKlineData].(map[string]interface{}); ok {
			return &ccxt.WatchOHLCV{
				Symbol:     getString(kData, FieldSymbol),
				Timeframe:  getString(kData, FieldKlineInterval),
				Timestamp:  getInt64(kData, FieldKlineStartTime),
				Open:       getFloat64(kData, FieldOpen),
				High:       getFloat64(kData, FieldHigh),
				Low:        getFloat64(kData, FieldLow),
				Close:      getFloat64(kData, FieldClose),
				Volume:     getFloat64(kData, FieldVolume),
				IsClosed:   getBool(kData, "x"),  // x表示K线是否闭合
				TradeCount: getInt64(kData, "n"), // n表示交易笔数
				StreamName: streamName,
			}
		}
	}
	return nil
}

// createChannel 创建通道
func (ws *WebSocket) createChannel(dataType string) interface{} {
	buffer := ws.config.ChannelBuffer
	switch dataType {
	case protocol.StreamEventMiniTicker:
		return make(chan *ccxt.WatchMiniTicker, buffer)
	case protocol.StreamEventMarkPrice:
		return make(chan *ccxt.WatchMarkPrice, buffer)
	case protocol.StreamEventBookTicker:
		return make(chan *ccxt.WatchBookTicker, buffer)
	case protocol.StreamEventOrderBook:
		return make(chan *ccxt.WatchOrderBook, buffer/2)
	case protocol.StreamEventTrade:
		return make(chan *ccxt.WatchTrade, buffer)
	case protocol.StreamEventKline:
		return make(chan *ccxt.WatchOHLCV, buffer/2)
	case protocol.StreamEventBalance:
		return make(chan *ccxt.WatchBalance, 100)
	case protocol.StreamEventOrders:
		return make(chan *ccxt.WatchOrder, 100)
	}
	return nil
}

// sendToChannel 发送到通道
func (ws *WebSocket) sendToChannel(channel interface{}, data interface{}) {
	switch ch := channel.(type) {
	case chan *ccxt.WatchMiniTicker:
		if ticker, ok := data.(*ccxt.WatchMiniTicker); ok {
			select {
			case ch <- ticker:
			default:
			}
		}
	case chan *ccxt.WatchMarkPrice:
		if markPrice, ok := data.(*ccxt.WatchMarkPrice); ok {
			select {
			case ch <- markPrice:
			default:
			}
		}
	case chan *ccxt.WatchBookTicker:
		if bookTicker, ok := data.(*ccxt.WatchBookTicker); ok {
			select {
			case ch <- bookTicker:
			default:
			}
		}
	case chan *ccxt.WatchOrderBook:
		if orderBook, ok := data.(*ccxt.WatchOrderBook); ok {
			select {
			case ch <- orderBook:
			default:
			}
		}
	case chan *ccxt.WatchTrade:
		if trade, ok := data.(*ccxt.WatchTrade); ok {
			select {
			case ch <- trade:
			default:
			}
		}
	case chan *ccxt.WatchOHLCV:
		if ohlcv, ok := data.(*ccxt.WatchOHLCV); ok {
			select {
			case ch <- ohlcv:
			default:
			}
		}
	}
}

// closeChannel 关闭通道
func (ws *WebSocket) closeChannel(channel interface{}) {
	switch ch := channel.(type) {
	case chan *ccxt.WatchMiniTicker:
		close(ch)
	case chan *ccxt.WatchMarkPrice:
		close(ch)
	case chan *ccxt.WatchBookTicker:
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

// closeConnection 关闭连接
func (ws *WebSocket) closeConnection(conn *WSConnection) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	if conn.ws != nil {
		conn.ws.Close()
	}
	atomic.StoreInt32(&conn.isHealthy, 0)
}

// getWebSocketURL 获取WebSocket URL
func (ws *WebSocket) getWebSocketURL() string {
	if wsURL, exists := ws.exchange.endpoints["websocket"]; exists && wsURL != "" {
		return wsURL
	}
	return ""
}

// healthChecker 健康检查
func (ws *WebSocket) healthChecker() {
	defer ws.wg.Done()

	ticker := time.NewTicker(ws.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ws.ctx.Done():
			return
		case <-ticker.C:
			ws.performHealthCheck()
		}
	}
}

// performHealthCheck 执行健康检查
func (ws *WebSocket) performHealthCheck() {
	ws.connMutex.RLock()
	defer ws.connMutex.RUnlock()

	var healthyCount int
	for _, conn := range ws.connections {
		if ws.checkConnectionHealth(conn) {
			healthyCount++
		}
	}

	logger.Debug("Health check completed",
		zap.Int("healthy_connections", healthyCount),
		zap.Int("total_connections", len(ws.connections)))
}

// checkConnectionHealth 检查连接健康
func (ws *WebSocket) checkConnectionHealth(conn *WSConnection) bool {
	conn.mu.RLock()
	defer conn.mu.RUnlock()

	if conn.ws == nil {
		atomic.StoreInt32(&conn.isHealthy, 0)
		return false
	}

	atomic.StoreInt32(&conn.isHealthy, 1)
	return true
}

// cleaner 清理器
func (ws *WebSocket) cleaner() {
	defer ws.wg.Done()

	ticker := time.NewTicker(ws.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ws.ctx.Done():
			return
		case <-ticker.C:
			ws.performCleanup()
		}
	}
}

// performCleanup 执行清理
func (ws *WebSocket) performCleanup() {
	now := time.Now()
	cleanedCount := 0

	ws.streams.Range(func(key, value interface{}) bool {
		if streamData, ok := value.(*StreamData); ok {
			if atomic.LoadInt32(&streamData.SubCount) == 0 &&
				now.Sub(streamData.LastUsed) > ws.config.IdleTimeout {
				ws.streams.Delete(key)
				cleanedCount++
			}
		}
		return true
	})

	if cleanedCount > 0 {
		logger.Info("Cleanup completed", zap.Int("cleaned_streams", cleanedCount))
	}
}

// GetStats 获取统计信息
func (ws *WebSocket) GetStats() map[string]interface{} {
	msgCount := atomic.LoadInt64(&ws.msgCount)
	errorCount := atomic.LoadInt64(&ws.errorCount)
	lastMsgTime := atomic.LoadInt64(&ws.lastMsgTime)

	var errorRate float64
	if msgCount > 0 {
		errorRate = float64(errorCount) / float64(msgCount) * 100
	}

	var activeStreams, totalSubs int32
	ws.streams.Range(func(key, value interface{}) bool {
		activeStreams++
		if streamData, ok := value.(*StreamData); ok {
			totalSubs += atomic.LoadInt32(&streamData.SubCount)
		}
		return true
	})

	ws.connMutex.RLock()
	connCount := len(ws.connections)
	ws.connMutex.RUnlock()

	// 获取消息频率限制器状态
	rateLimiterStatus := ws.msgRateLimiter.GetStatus()

	return map[string]interface{}{
		"is_running":          atomic.LoadInt32(&ws.isRunning) == 1,
		"connections":         connCount,
		"active_streams":      activeStreams,
		"total_subscriptions": totalSubs,
		"messages_received":   msgCount,
		"errors_count":        errorCount,
		"last_message_time":   lastMsgTime,
		"error_rate":          errorRate,
		"rate_limiter":        rateLimiterStatus,
	}
}

// ========== 便捷方法 ==========

// SubscribeMiniTicker 订阅轻量级ticker
func (ws *WebSocket) SubscribeMiniTicker(symbol string, params map[string]interface{}) (string, <-chan *ccxt.WatchMiniTicker, error) {
	// 从params中解析流名称
	var streamName string
	if params != nil {
		streamName = cast.ToString(params[ParamStream])
	}
	if streamName == "" {
		return "", nil, fmt.Errorf("stream name is required in params[%s]", ParamStream)
	}

	subID, channel, err := ws.Subscribe(streamName, protocol.StreamEventMiniTicker)
	if err != nil {
		return "", nil, err
	}
	ch := channel.(chan *ccxt.WatchMiniTicker)
	return subID, ch, nil
}

// SubscribeMarkPrice 订阅标记价格
func (ws *WebSocket) SubscribeMarkPrice(symbol string, params map[string]interface{}) (string, <-chan *ccxt.WatchMarkPrice, error) {
	// 从params中解析流名称
	var streamName string
	if params != nil {
		streamName = cast.ToString(params[ParamStream])
	}
	if streamName == "" {
		return "", nil, fmt.Errorf("stream name is required in params[%s]", ParamStream)
	}

	subID, channel, err := ws.Subscribe(streamName, protocol.StreamEventMarkPrice)
	if err != nil {
		return "", nil, err
	}
	ch := channel.(chan *ccxt.WatchMarkPrice)
	return subID, ch, nil
}

// SubscribeBookTicker 订阅最优买卖价
func (ws *WebSocket) SubscribeBookTicker(symbol string, params map[string]interface{}) (string, <-chan *ccxt.WatchBookTicker, error) {
	// 从params中解析流名称
	var streamName string
	if params != nil {
		streamName = cast.ToString(params[ParamStream])
	}
	if streamName == "" {
		return "", nil, fmt.Errorf("stream name is required in params[%s]", ParamStream)
	}

	subID, channel, err := ws.Subscribe(streamName, protocol.StreamEventBookTicker)
	if err != nil {
		return "", nil, err
	}
	ch := channel.(chan *ccxt.WatchBookTicker)
	return subID, ch, nil
}

// SubscribeOrderBook 订阅订单簿
func (ws *WebSocket) SubscribeOrderBook(symbol string, params map[string]interface{}) (string, <-chan *ccxt.WatchOrderBook, error) {
	// 从params中解析流名称
	var streamName string
	if params != nil {
		streamName = cast.ToString(params[ParamStream])
	}
	if streamName == "" {
		return "", nil, fmt.Errorf("stream name is required in params[%s]", ParamStream)
	}

	subID, channel, err := ws.Subscribe(streamName, protocol.StreamEventOrderBook)
	if err != nil {
		return "", nil, err
	}
	ch := channel.(chan *ccxt.WatchOrderBook)
	return subID, ch, nil
}

// SubscribeTrades 订阅交易数据
func (ws *WebSocket) SubscribeTrades(symbol string, params map[string]interface{}) (string, <-chan *ccxt.WatchTrade, error) {
	// 从params中解析流名称
	var streamName string
	if params != nil {
		streamName = cast.ToString(params[ParamStream])
	}
	if streamName == "" {
		return "", nil, fmt.Errorf("stream name is required in params[%s]", ParamStream)
	}

	subID, channel, err := ws.Subscribe(streamName, protocol.StreamEventTrade)
	if err != nil {
		return "", nil, err
	}
	ch := channel.(chan *ccxt.WatchTrade)
	return subID, ch, nil
}

// SubscribeKlines 订阅K线数据
func (ws *WebSocket) SubscribeKlines(symbol, interval string, params map[string]interface{}) (string, <-chan *ccxt.WatchOHLCV, error) {
	// 从params中解析流名称
	var streamName string
	if params != nil {
		streamName = cast.ToString(params[ParamStream])
	}
	if streamName == "" {
		return "", nil, fmt.Errorf("stream name is required in params[%s]", ParamStream)
	}

	subID, channel, err := ws.Subscribe(streamName, protocol.StreamEventKline)
	if err != nil {
		return "", nil, err
	}
	ch := channel.(chan *ccxt.WatchOHLCV)
	return subID, ch, nil
}

// ========== 辅助函数 ==========

// getString 从map中获取字符串值
func getString(data map[string]interface{}, key string) string {
	if v, ok := data[key].(string); ok {
		return v
	}
	return ""
}

// getInt64 从map中获取int64值
func getInt64(data map[string]interface{}, key string) int64 {
	if v, ok := data[key].(float64); ok {
		return int64(v)
	}
	if v, ok := data[key].(int64); ok {
		return v
	}
	return 0
}

// getFloat64 从map中获取float64值
func getFloat64(data map[string]interface{}, key string) float64 {
	if v, ok := data[key].(float64); ok {
		return v
	}
	if v, ok := data[key].(string); ok {
		// 尝试解析字符串为float64
		if f, err := json.Number(v).Float64(); err == nil {
			return f
		}
	}
	return 0
}

// getFloat64Interface 从interface{}中获取float64值
func getFloat64Interface(v interface{}) float64 {
	if f, ok := v.(float64); ok {
		return f
	}
	if s, ok := v.(string); ok {
		if f, err := json.Number(s).Float64(); err == nil {
			return f
		}
	}
	return 0
}

// getBool 从map中获取bool值
func getBool(data map[string]interface{}, key string) bool {
	if v, ok := data[key].(bool); ok {
		return v
	}
	return false
}
