package client

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/riven-blade/datahive/pkg/logger"
	"github.com/riven-blade/datahive/pkg/protocol"
	"github.com/riven-blade/datahive/pkg/protocol/pb"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// DataHiveClient DataHive客户端接口
type DataHiveClient interface {
	// 连接管理
	Connect(address string) error
	Disconnect() error
	IsConnected() bool

	// 获取数据 (RESTful)
	FetchMarkets(ctx context.Context, exchange, marketType, stackType string, reload ...bool) (map[string]*pb.Market, error)
	FetchTicker(ctx context.Context, exchange, marketType, symbol string) (*pb.Ticker, error)
	FetchTickers(ctx context.Context, exchange, marketType string, symbols ...string) ([]*pb.Ticker, error)
	FetchOHLCV(ctx context.Context, exchange, marketType, symbol, timeframe string, since int64, limit int32) ([]*pb.Kline, error)
	FetchTrades(ctx context.Context, exchange, marketType, symbol string, since int64, limit int32) ([]*pb.Trade, error)
	FetchOrderBook(ctx context.Context, exchange, marketType, symbol string, limit int32) (*pb.OrderBook, error)

	// 实时订阅 (WebSocket)
	WatchMiniTicker(ctx context.Context, exchange, marketType, symbol string) (<-chan *pb.MiniTickerUpdate, string, error)
	WatchMarkPrice(ctx context.Context, exchange, marketType, symbol string) (<-chan *pb.MarkPriceUpdate, string, error)
	WatchBookTicker(ctx context.Context, exchange, marketType, symbol string) (<-chan *pb.BookTickerUpdate, string, error)
	WatchKline(ctx context.Context, exchange, marketType, symbol, timeframe string) (<-chan *pb.KlineUpdate, string, error)
	WatchTrades(ctx context.Context, exchange, marketType, symbol string) (<-chan *pb.TradeUpdate, string, error)
	WatchOrderBook(ctx context.Context, exchange, marketType, symbol string, limit int32) (<-chan *pb.OrderBookUpdate, string, error)
	Unwatch(subscriptionID string) error

	// 状态和错误处理
	SetErrorHandler(handler func(error))
	GetStats() *ClientStats
	GetSubscriptions() map[string]map[string]interface{}
	GetSubscriptionStats() map[string]interface{}
}

// SubscriptionState 订阅状态
type SubscriptionState int32

const (
	SubscriptionPending SubscriptionState = iota
	SubscriptionActive
	SubscriptionClosed
	SubscriptionError
)

func (s SubscriptionState) String() string {
	switch s {
	case SubscriptionPending:
		return "pending"
	case SubscriptionActive:
		return "active"
	case SubscriptionClosed:
		return "closed"
	case SubscriptionError:
		return "error"
	default:
		return "unknown"
	}
}

// subscription 订阅信息
type subscription struct {
	ID       string
	Type     string // miniTicker, mark_price, bookTicker, kline, trades, orderbook
	Exchange string
	Symbol   string

	// 状态管理
	state     int32 // SubscriptionState
	createdAt time.Time
	lastData  time.Time
	dataCount int64

	// Channels
	MiniTickerCh chan *pb.MiniTickerUpdate
	MarkPriceCh  chan *pb.MarkPriceUpdate
	BookTickerCh chan *pb.BookTickerUpdate
	KlineCh      chan *pb.KlineUpdate
	TradesCh     chan *pb.TradeUpdate
	OrderBookCh  chan *pb.OrderBookUpdate

	// 生命周期
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.RWMutex
}

// GetState 获取订阅状态
func (s *subscription) GetState() SubscriptionState {
	return SubscriptionState(atomic.LoadInt32(&s.state))
}

// SetState 设置订阅状态
func (s *subscription) SetState(state SubscriptionState) {
	atomic.StoreInt32(&s.state, int32(state))
}

// UpdateLastData 更新最后数据接收时间
func (s *subscription) UpdateLastData() {
	s.mu.Lock()
	s.lastData = time.Now()
	s.dataCount++
	s.mu.Unlock()
}

// GetStats 获取订阅统计信息
func (s *subscription) GetStats() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return map[string]interface{}{
		"id":         s.ID,
		"type":       s.Type,
		"exchange":   s.Exchange,
		"symbol":     s.Symbol,
		"state":      s.GetState().String(),
		"created_at": s.createdAt,
		"last_data":  s.lastData,
		"data_count": s.dataCount,
	}
}

// IsActive 检查订阅是否活跃
func (s *subscription) IsActive() bool {
	return s.GetState() == SubscriptionActive
}

// dataHiveClient DataHive客户端实现
type dataHiveClient struct {
	client Client
	logger *logger.MLogger

	// 订阅管理
	subscriptions map[string]*subscription
	subMutex      sync.RWMutex

	// 错误处理
	errorHandler func(error)
	errorMu      sync.RWMutex
}

// NewDataHiveClient 创建新的DataHive客户端
func NewDataHiveClient(client Client) DataHiveClient {
	dhClient := &dataHiveClient{
		client:        client,
		logger:        &logger.MLogger{Logger: logger.L()},
		subscriptions: make(map[string]*subscription),
	}

	// 注册消息处理器
	dhClient.registerHandlers()

	return dhClient
}

// Connect 连接到服务器
func (c *dataHiveClient) Connect(address string) error {
	c.logger.Info("Connecting to DataHive server", zap.String("address", address))

	err := c.client.Connect(address)
	if err != nil {
		c.logger.Error("Failed to connect to DataHive server",
			zap.String("address", address),
			zap.Error(err))
		return err
	}

	c.logger.Info("Successfully connected to DataHive server", zap.String("address", address))
	return nil
}

// Disconnect 断开连接
func (c *dataHiveClient) Disconnect() error {
	c.logger.Info("Disconnecting from DataHive server")

	// 取消所有订阅
	c.subMutex.Lock()
	subscriptionCount := len(c.subscriptions)
	for id, sub := range c.subscriptions {
		c.closeSubscription(sub)
		delete(c.subscriptions, id)
	}
	c.subMutex.Unlock()

	c.logger.Info("Closed all subscriptions", zap.Int("count", subscriptionCount))

	err := c.client.Disconnect()
	if err != nil {
		c.logger.Error("Failed to disconnect from DataHive server", zap.Error(err))
		return err
	}

	c.logger.Info("Successfully disconnected from DataHive server")
	return nil
}

// IsConnected 检查连接状态
func (c *dataHiveClient) IsConnected() bool {
	return c.client.IsConnected()
}

// SetErrorHandler 设置错误处理器
func (c *dataHiveClient) SetErrorHandler(handler func(error)) {
	c.errorMu.Lock()
	defer c.errorMu.Unlock()
	c.errorHandler = handler
}

// GetStats 获取统计信息
func (c *dataHiveClient) GetStats() *ClientStats {
	return c.client.GetStats()
}

// GetSubscriptions 获取所有订阅信息
func (c *dataHiveClient) GetSubscriptions() map[string]map[string]interface{} {
	c.subMutex.RLock()
	defer c.subMutex.RUnlock()

	subscriptions := make(map[string]map[string]interface{})
	for id, sub := range c.subscriptions {
		subscriptions[id] = sub.GetStats()
	}

	return subscriptions
}

// GetSubscriptionStats 获取订阅统计汇总
func (c *dataHiveClient) GetSubscriptionStats() map[string]interface{} {
	c.subMutex.RLock()
	defer c.subMutex.RUnlock()

	stats := map[string]interface{}{
		"total_subscriptions":       len(c.subscriptions),
		"active_subscriptions":      0,
		"pending_subscriptions":     0,
		"closed_subscriptions":      0,
		"error_subscriptions":       0,
		"subscriptions_by_type":     make(map[string]int),
		"subscriptions_by_exchange": make(map[string]int),
	}

	typeStats := stats["subscriptions_by_type"].(map[string]int)
	exchangeStats := stats["subscriptions_by_exchange"].(map[string]int)

	for _, sub := range c.subscriptions {
		switch sub.GetState() {
		case SubscriptionActive:
			stats["active_subscriptions"] = stats["active_subscriptions"].(int) + 1
		case SubscriptionPending:
			stats["pending_subscriptions"] = stats["pending_subscriptions"].(int) + 1
		case SubscriptionClosed:
			stats["closed_subscriptions"] = stats["closed_subscriptions"].(int) + 1
		case SubscriptionError:
			stats["error_subscriptions"] = stats["error_subscriptions"].(int) + 1
		}

		typeStats[sub.Type]++
		exchangeStats[sub.Exchange]++
	}

	return stats
}

// =============================================================================
// Fetch Methods (RESTful API)
// =============================================================================

// FetchMarkets 获取市场信息
func (c *dataHiveClient) FetchMarkets(ctx context.Context, exchange, marketType, stackType string, reload ...bool) (map[string]*pb.Market, error) {
	req := &pb.FetchMarketsRequest{
		Exchange:   exchange,
		MarketType: marketType,
		StackType:  stackType,
	}

	data, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := c.client.SendRequestWithTimeout(pb.ActionType_FETCH_MARKETS, data, 30*time.Second)
	if err != nil {
		return nil, err
	}

	var marketsResp pb.FetchMarketsResponse
	if err := proto.Unmarshal(resp.Data, &marketsResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	// 将切片转换为 map，key 为 symbol
	markets := make(map[string]*pb.Market)
	for i := range marketsResp.Markets {
		markets[marketsResp.Markets[i].Symbol] = marketsResp.Markets[i]
	}

	return markets, nil
}

// FetchTicker 获取单个ticker
func (c *dataHiveClient) FetchTicker(ctx context.Context, exchange, marketType, symbol string) (*pb.Ticker, error) {
	req := &pb.FetchTickerRequest{
		Exchange:   exchange,
		MarketType: marketType,
		Symbol:     symbol,
	}

	data, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := c.client.SendRequestWithTimeout(pb.ActionType_FETCH_TICKER, data, 30*time.Second)
	if err != nil {
		return nil, err
	}

	var tickerResp pb.FetchTickerResponse
	if err := proto.Unmarshal(resp.Data, &tickerResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return tickerResp.Ticker, nil
}

// FetchTickers 获取多个tickers
func (c *dataHiveClient) FetchTickers(ctx context.Context, exchange, marketType string, symbols ...string) ([]*pb.Ticker, error) {
	if len(symbols) == 0 {
		return nil, fmt.Errorf("at least one symbol is required")
	}

	var tickers []*pb.Ticker

	// 现在单独处理每个symbol
	for _, symbol := range symbols {
		req := &pb.FetchTickersRequest{
			Exchange:   exchange,
			MarketType: marketType,
			Symbol:     symbol,
		}

		data, err := proto.Marshal(req)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request for %s: %w", symbol, err)
		}

		resp, err := c.client.SendRequestWithTimeout(pb.ActionType_FETCH_TICKERS, data, 30*time.Second)
		if err != nil {
			return nil, err
		}

		var tickersResp pb.FetchTickersResponse
		if err := proto.Unmarshal(resp.Data, &tickersResp); err != nil {
			return nil, fmt.Errorf("failed to unmarshal response for %s: %w", symbol, err)
		}

		tickers = append(tickers, tickersResp.Ticker)
	}

	return tickers, nil
}

// FetchOHLCV 获取K线数据
func (c *dataHiveClient) FetchOHLCV(ctx context.Context, exchange, marketType, symbol, timeframe string, since int64, limit int32) ([]*pb.Kline, error) {
	req := &pb.FetchKlinesRequest{
		Exchange:   exchange,
		MarketType: marketType,
		Symbol:     symbol,
		Interval:   timeframe,
		StartTime:  since,
		Limit:      limit,
	}

	data, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := c.client.SendRequestWithTimeout(pb.ActionType_FETCH_KLINES, data, 30*time.Second)
	if err != nil {
		return nil, err
	}

	var klinesResp pb.FetchKlinesResponse
	if err := proto.Unmarshal(resp.Data, &klinesResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return klinesResp.Klines, nil
}

// FetchTrades 获取交易数据
func (c *dataHiveClient) FetchTrades(ctx context.Context, exchange, marketType, symbol string, since int64, limit int32) ([]*pb.Trade, error) {
	req := &pb.FetchTradesRequest{
		Exchange:   exchange,
		MarketType: marketType,
		Symbol:     symbol,
		Since:      since,
		Limit:      limit,
	}

	data, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := c.client.SendRequestWithTimeout(pb.ActionType_FETCH_TRADES, data, 30*time.Second)
	if err != nil {
		return nil, err
	}

	var tradesResp pb.FetchTradesResponse
	if err := proto.Unmarshal(resp.Data, &tradesResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return tradesResp.Trades, nil
}

// FetchOrderBook 获取订单簿
func (c *dataHiveClient) FetchOrderBook(ctx context.Context, exchange, marketType, symbol string, limit int32) (*pb.OrderBook, error) {
	req := &pb.FetchOrderBookRequest{
		Exchange:   exchange,
		MarketType: marketType,
		Symbol:     symbol,
		Limit:      limit,
	}

	data, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := c.client.SendRequestWithTimeout(pb.ActionType_FETCH_ORDERBOOK, data, 30*time.Second)
	if err != nil {
		return nil, err
	}

	var orderBookResp pb.FetchOrderBookResponse
	if err := proto.Unmarshal(resp.Data, &orderBookResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return orderBookResp.Orderbook, nil
}

// =============================================================================
// Watch Methods (Real-time WebSocket)
// =============================================================================

// WatchKline 订阅K线更新
func (c *dataHiveClient) WatchKline(ctx context.Context, exchange, marketType, symbol, timeframe string) (<-chan *pb.KlineUpdate, string, error) {
	c.logger.Info("Starting kline subscription",
		zap.String("exchange", exchange),
		zap.String("market_type", marketType),
		zap.String("symbol", symbol),
		zap.String("timeframe", timeframe))

	req := &pb.SubscribeRequest{
		Exchange:      exchange,
		MarketType:    marketType,
		Symbol:        symbol,
		DataType:      pb.DataType_KLINE,
		KlineInterval: timeframe,
	}

	data, err := proto.Marshal(req)
	if err != nil {
		return nil, "", fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := c.client.SendRequestWithTimeout(pb.ActionType_SUBSCRIBE, data, 30*time.Second)
	if err != nil {
		return nil, "", err
	}

	var subscribeResp pb.SubscribeResponse
	if err := proto.Unmarshal(resp.Data, &subscribeResp); err != nil {
		return nil, "", fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if !subscribeResp.Success {
		return nil, "", fmt.Errorf("subscription failed: %s", subscribeResp.Message)
	}

	// 创建订阅
	klineCh := make(chan *pb.KlineUpdate, 100)
	subCtx, cancel := context.WithCancel(ctx)

	sub := &subscription{
		ID:        subscribeResp.Topic,
		Type:      protocol.StreamEventKline,
		Exchange:  exchange,
		Symbol:    symbol,
		state:     int32(SubscriptionPending),
		createdAt: time.Now(),
		KlineCh:   klineCh,
		ctx:       subCtx,
		cancel:    cancel,
		done:      make(chan struct{}),
	}

	// 标记为活跃状态
	atomic.StoreInt32(&sub.state, int32(SubscriptionActive))

	c.subMutex.Lock()
	c.subscriptions[sub.ID] = sub
	c.subMutex.Unlock()

	c.logger.Info("Kline subscription created successfully",
		zap.String("subscription_id", sub.ID),
		zap.String("exchange", exchange),
		zap.String("symbol", symbol),
		zap.String("timeframe", timeframe))

	return klineCh, sub.ID, nil
}

// WatchTrades 订阅交易更新
func (c *dataHiveClient) WatchTrades(ctx context.Context, exchange, marketType, symbol string) (<-chan *pb.TradeUpdate, string, error) {
	req := &pb.SubscribeRequest{
		Exchange:   exchange,
		MarketType: marketType,
		Symbol:     symbol,
		DataType:   pb.DataType_TRADE,
	}

	data, err := proto.Marshal(req)
	if err != nil {
		return nil, "", fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := c.client.SendRequestWithTimeout(pb.ActionType_SUBSCRIBE, data, 30*time.Second)
	if err != nil {
		return nil, "", err
	}

	var subscribeResp pb.SubscribeResponse
	if err := proto.Unmarshal(resp.Data, &subscribeResp); err != nil {
		return nil, "", fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if !subscribeResp.Success {
		return nil, "", fmt.Errorf("subscription failed: %s", subscribeResp.Message)
	}

	// 创建订阅
	tradesCh := make(chan *pb.TradeUpdate, 100)
	_, cancel := context.WithCancel(ctx)

	sub := &subscription{
		ID:       subscribeResp.Topic,
		Type:     protocol.StreamEventTrade,
		Exchange: exchange,
		Symbol:   symbol,
		TradesCh: tradesCh,
		cancel:   cancel,
		done:     make(chan struct{}),
	}

	c.subMutex.Lock()
	c.subscriptions[sub.ID] = sub
	c.subMutex.Unlock()

	return tradesCh, sub.ID, nil
}

// WatchOrderBook 订阅订单簿更新
func (c *dataHiveClient) WatchOrderBook(ctx context.Context, exchange, marketType, symbol string, limit int32) (<-chan *pb.OrderBookUpdate, string, error) {
	req := &pb.SubscribeRequest{
		Exchange:       exchange,
		MarketType:     marketType,
		Symbol:         symbol,
		DataType:       pb.DataType_ORDERBOOK,
		OrderbookDepth: limit,
	}

	data, err := proto.Marshal(req)
	if err != nil {
		return nil, "", fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := c.client.SendRequestWithTimeout(pb.ActionType_SUBSCRIBE, data, 30*time.Second)
	if err != nil {
		return nil, "", err
	}

	var subscribeResp pb.SubscribeResponse
	if err := proto.Unmarshal(resp.Data, &subscribeResp); err != nil {
		return nil, "", fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if !subscribeResp.Success {
		return nil, "", fmt.Errorf("subscription failed: %s", subscribeResp.Message)
	}

	// 创建订阅
	orderBookCh := make(chan *pb.OrderBookUpdate, 100)
	_, cancel := context.WithCancel(ctx)

	sub := &subscription{
		ID:          subscribeResp.Topic,
		Type:        protocol.StreamEventOrderBook,
		Exchange:    exchange,
		Symbol:      symbol,
		OrderBookCh: orderBookCh,
		cancel:      cancel,
		done:        make(chan struct{}),
	}

	c.subMutex.Lock()
	c.subscriptions[sub.ID] = sub
	c.subMutex.Unlock()

	return orderBookCh, sub.ID, nil
}

// =====================================================================================
// 新增的Watch方法 - 支持增强协议
// =====================================================================================

// WatchMiniTicker 订阅轻量级ticker更新
func (c *dataHiveClient) WatchMiniTicker(ctx context.Context, exchange, marketType, symbol string) (<-chan *pb.MiniTickerUpdate, string, error) {
	req := &pb.SubscribeRequest{
		Exchange:   exchange,
		MarketType: marketType,
		Symbol:     symbol,
		DataType:   pb.DataType_MINI_TICKER,
	}

	data, err := proto.Marshal(req)
	if err != nil {
		return nil, "", fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := c.client.SendRequestWithTimeout(pb.ActionType_SUBSCRIBE, data, 30*time.Second)
	if err != nil {
		return nil, "", err
	}

	var subscribeResp pb.SubscribeResponse
	if err := proto.Unmarshal(resp.Data, &subscribeResp); err != nil {
		return nil, "", fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if !subscribeResp.Success {
		return nil, "", fmt.Errorf("subscription failed: %s", subscribeResp.Message)
	}

	// 创建订阅
	miniTickerCh := make(chan *pb.MiniTickerUpdate, 100)
	_, cancel := context.WithCancel(ctx)

	sub := &subscription{
		ID:           subscribeResp.Topic,
		Type:         protocol.StreamEventMiniTicker,
		Exchange:     exchange,
		Symbol:       symbol,
		MiniTickerCh: miniTickerCh,
		cancel:       cancel,
		done:         make(chan struct{}),
	}

	c.subMutex.Lock()
	c.subscriptions[sub.ID] = sub
	c.subMutex.Unlock()

	return miniTickerCh, sub.ID, nil
}

// WatchMarkPrice 订阅标记价格更新
func (c *dataHiveClient) WatchMarkPrice(ctx context.Context, exchange, marketType, symbol string) (<-chan *pb.MarkPriceUpdate, string, error) {
	req := &pb.SubscribeRequest{
		Exchange:   exchange,
		MarketType: marketType,
		Symbol:     symbol,
		DataType:   pb.DataType_MARK_PRICE,
	}

	data, err := proto.Marshal(req)
	if err != nil {
		return nil, "", fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := c.client.SendRequestWithTimeout(pb.ActionType_SUBSCRIBE, data, 30*time.Second)
	if err != nil {
		return nil, "", err
	}

	var subscribeResp pb.SubscribeResponse
	if err := proto.Unmarshal(resp.Data, &subscribeResp); err != nil {
		return nil, "", fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if !subscribeResp.Success {
		return nil, "", fmt.Errorf("subscription failed: %s", subscribeResp.Message)
	}

	// 创建订阅
	markPriceCh := make(chan *pb.MarkPriceUpdate, 100)
	_, cancel := context.WithCancel(ctx)

	sub := &subscription{
		ID:          subscribeResp.Topic,
		Type:        protocol.StreamEventMarkPrice,
		Exchange:    exchange,
		Symbol:      symbol,
		MarkPriceCh: markPriceCh,
		cancel:      cancel,
		done:        make(chan struct{}),
	}

	c.subMutex.Lock()
	c.subscriptions[sub.ID] = sub
	c.subMutex.Unlock()

	return markPriceCh, sub.ID, nil
}

// WatchBookTicker 订阅最优买卖价更新
func (c *dataHiveClient) WatchBookTicker(ctx context.Context, exchange, marketType, symbol string) (<-chan *pb.BookTickerUpdate, string, error) {
	req := &pb.SubscribeRequest{
		Exchange:   exchange,
		MarketType: marketType,
		Symbol:     symbol,
		DataType:   pb.DataType_BOOK_TICKER,
	}

	data, err := proto.Marshal(req)
	if err != nil {
		return nil, "", fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := c.client.SendRequestWithTimeout(pb.ActionType_SUBSCRIBE, data, 30*time.Second)
	if err != nil {
		return nil, "", err
	}

	var subscribeResp pb.SubscribeResponse
	if err := proto.Unmarshal(resp.Data, &subscribeResp); err != nil {
		return nil, "", fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if !subscribeResp.Success {
		return nil, "", fmt.Errorf("subscription failed: %s", subscribeResp.Message)
	}

	// 创建订阅
	bookTickerCh := make(chan *pb.BookTickerUpdate, 100)
	_, cancel := context.WithCancel(ctx)

	sub := &subscription{
		ID:           subscribeResp.Topic,
		Type:         protocol.StreamEventBookTicker,
		Exchange:     exchange,
		Symbol:       symbol,
		BookTickerCh: bookTickerCh,
		cancel:       cancel,
		done:         make(chan struct{}),
	}

	c.subMutex.Lock()
	c.subscriptions[sub.ID] = sub
	c.subMutex.Unlock()

	return bookTickerCh, sub.ID, nil
}

// Unwatch 取消订阅
func (c *dataHiveClient) Unwatch(subscriptionID string) error {
	c.logger.Info("Canceling subscription", zap.String("subscription_id", subscriptionID))

	c.subMutex.Lock()
	sub, exists := c.subscriptions[subscriptionID]
	if !exists {
		c.subMutex.Unlock()
		c.logger.Warn("Subscription not found", zap.String("subscription_id", subscriptionID))
		return fmt.Errorf("subscription not found: %s", subscriptionID)
	}
	delete(c.subscriptions, subscriptionID)
	c.subMutex.Unlock()

	c.logger.Info("Removed subscription from map",
		zap.String("subscription_id", subscriptionID),
		zap.String("type", sub.Type),
		zap.String("symbol", sub.Symbol))

	// 发送取消订阅请求
	req := &pb.UnsubscribeRequest{
		Topic: subscriptionID,
	}

	data, err := proto.Marshal(req)
	if err != nil {
		c.logger.Error("Failed to marshal unsubscribe request", zap.Error(err))
	} else {
		_, err = c.client.SendRequestWithTimeout(pb.ActionType_UNSUBSCRIBE, data, 30*time.Second)
		if err != nil {
			c.logger.Error("Failed to send unsubscribe request", zap.Error(err))
		}
	}

	// 关闭订阅
	c.closeSubscription(sub)

	c.logger.Info("Successfully unsubscribed",
		zap.String("subscription_id", subscriptionID),
		zap.String("type", sub.Type),
		zap.String("symbol", sub.Symbol))
	return nil
}

// =============================================================================
// Internal Message Handling
// =============================================================================

// registerHandlers 注册消息处理器
func (c *dataHiveClient) registerHandlers() {
	c.client.RegisterHandler(pb.ActionType_KLINE_UPDATE, c.handleKlineUpdate)
	c.client.RegisterHandler(pb.ActionType_TRADE_UPDATE, c.handleTradeUpdate)
	c.client.RegisterHandler(pb.ActionType_ORDERBOOK_UPDATE, c.handleOrderBookUpdate)
	c.client.RegisterHandler(pb.ActionType_MINI_TICKER_UPDATE, c.handleMiniTickerUpdate)
	c.client.RegisterHandler(pb.ActionType_MARK_PRICE_UPDATE, c.handleMarkPriceUpdate)
	c.client.RegisterHandler(pb.ActionType_BOOK_TICKER_UPDATE, c.handleBookTickerUpdate)
}

// handleMiniTickerUpdate 处理迷你ticker更新消息
func (c *dataHiveClient) handleMiniTickerUpdate(msg *pb.Message) error {
	var update pb.MiniTickerUpdate
	if err := proto.Unmarshal(msg.Data, &update); err != nil {
		return fmt.Errorf("failed to unmarshal mini ticker update: %w", err)
	}

	c.subMutex.RLock()
	sub, exists := c.subscriptions[update.Topic]
	c.subMutex.RUnlock()

	if !exists || sub.MiniTickerCh == nil || !sub.IsActive() {
		return nil
	}

	// 更新数据统计
	sub.UpdateLastData()

	select {
	case sub.MiniTickerCh <- &update:
		c.logger.Debug("Mini ticker update sent",
			zap.String("topic", update.Topic),
			zap.String("symbol", sub.Symbol))
	default:
		c.logger.Warn("Mini ticker channel full, dropping message",
			zap.String("topic", update.Topic),
			zap.String("symbol", sub.Symbol))
	}

	return nil
}

// handleMarkPriceUpdate 处理标记价格更新消息
func (c *dataHiveClient) handleMarkPriceUpdate(msg *pb.Message) error {
	var update pb.MarkPriceUpdate
	if err := proto.Unmarshal(msg.Data, &update); err != nil {
		return fmt.Errorf("failed to unmarshal mark price update: %w", err)
	}

	c.subMutex.RLock()
	sub, exists := c.subscriptions[update.Topic]
	c.subMutex.RUnlock()

	if !exists || sub.MarkPriceCh == nil {
		return nil
	}

	select {
	case sub.MarkPriceCh <- &update:
	default:
		c.logger.Warn("Mark price channel full, dropping message", zap.String("topic", update.Topic))
	}

	return nil
}

// handleBookTickerUpdate 处理最优买卖价更新消息
func (c *dataHiveClient) handleBookTickerUpdate(msg *pb.Message) error {
	var update pb.BookTickerUpdate
	if err := proto.Unmarshal(msg.Data, &update); err != nil {
		return fmt.Errorf("failed to unmarshal book ticker update: %w", err)
	}

	c.subMutex.RLock()
	sub, exists := c.subscriptions[update.Topic]
	c.subMutex.RUnlock()

	if !exists || sub.BookTickerCh == nil {
		return nil
	}

	select {
	case sub.BookTickerCh <- &update:
	default:
		c.logger.Warn("Book ticker channel full, dropping message", zap.String("topic", update.Topic))
	}

	return nil
}

// handleKlineUpdate 处理K线更新消息
func (c *dataHiveClient) handleKlineUpdate(msg *pb.Message) error {
	var update pb.KlineUpdate
	if err := proto.Unmarshal(msg.Data, &update); err != nil {
		return fmt.Errorf("failed to unmarshal kline update: %w", err)
	}

	c.subMutex.RLock()
	sub, exists := c.subscriptions[update.Topic]
	c.subMutex.RUnlock()

	if !exists || sub.KlineCh == nil {
		return nil
	}

	select {
	case sub.KlineCh <- &update:
	default:
		c.logger.Warn("Kline channel full, dropping message", zap.String("topic", update.Topic))
	}

	return nil
}

// handleTradeUpdate 处理交易更新消息
func (c *dataHiveClient) handleTradeUpdate(msg *pb.Message) error {
	var update pb.TradeUpdate
	if err := proto.Unmarshal(msg.Data, &update); err != nil {
		return fmt.Errorf("failed to unmarshal trade update: %w", err)
	}

	c.subMutex.RLock()
	sub, exists := c.subscriptions[update.Topic]
	c.subMutex.RUnlock()

	if !exists || sub.TradesCh == nil {
		return nil
	}

	select {
	case sub.TradesCh <- &update:
	default:
		c.logger.Warn("Trades channel full, dropping message", zap.String("topic", update.Topic))
	}

	return nil
}

// handleOrderBookUpdate 处理订单簿更新消息
func (c *dataHiveClient) handleOrderBookUpdate(msg *pb.Message) error {
	var update pb.OrderBookUpdate
	if err := proto.Unmarshal(msg.Data, &update); err != nil {
		return fmt.Errorf("failed to unmarshal orderbook update: %w", err)
	}

	c.subMutex.RLock()
	sub, exists := c.subscriptions[update.Topic]
	c.subMutex.RUnlock()

	if !exists || sub.OrderBookCh == nil {
		return nil
	}

	select {
	case sub.OrderBookCh <- &update:
	default:
		c.logger.Warn("OrderBook channel full, dropping message", zap.String("topic", update.Topic))
	}

	return nil
}

// =============================================================================
// Internal Helper Methods
// =============================================================================

// closeSubscription 关闭订阅
func (c *dataHiveClient) closeSubscription(sub *subscription) {
	// 设置为关闭状态
	sub.SetState(SubscriptionClosed)

	if sub.cancel != nil {
		sub.cancel()
	}

	// 关闭channels
	if sub.MiniTickerCh != nil {
		close(sub.MiniTickerCh)
	}
	if sub.MarkPriceCh != nil {
		close(sub.MarkPriceCh)
	}
	if sub.BookTickerCh != nil {
		close(sub.BookTickerCh)
	}
	if sub.KlineCh != nil {
		close(sub.KlineCh)
	}
	if sub.TradesCh != nil {
		close(sub.TradesCh)
	}
	if sub.OrderBookCh != nil {
		close(sub.OrderBookCh)
	}
	if sub.done != nil {
		close(sub.done)
	}

	c.logger.Debug("Subscription closed",
		zap.String("id", sub.ID),
		zap.String("type", sub.Type),
		zap.String("symbol", sub.Symbol))
}

// notifyError 通知错误
func (c *dataHiveClient) notifyError(err error) {
	c.errorMu.RLock()
	handler := c.errorHandler
	c.errorMu.RUnlock()

	if handler != nil {
		go handler(err)
	}

	c.logger.Error("DataHive client error", zap.Error(err))
}
