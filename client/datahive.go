package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/riven-blade/datahive/pkg/logger"
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
	WatchPrice(ctx context.Context, exchange, marketType, symbol string) (<-chan *pb.PriceUpdate, string, error)
	WatchKline(ctx context.Context, exchange, marketType, symbol, timeframe string) (<-chan *pb.KlineUpdate, string, error)
	WatchTrades(ctx context.Context, exchange, marketType, symbol string) (<-chan *pb.TradeUpdate, string, error)
	WatchOrderBook(ctx context.Context, exchange, marketType, symbol string, limit int32) (<-chan *pb.OrderBookUpdate, string, error)
	Unwatch(subscriptionID string) error

	// 状态和错误处理
	SetErrorHandler(handler func(error))
	GetStats() *ClientStats
}

// subscription 订阅信息
type subscription struct {
	ID       string
	Type     string // price, kline, trades, orderbook
	Exchange string
	Symbol   string

	// Channels
	PriceCh     chan *pb.PriceUpdate
	KlineCh     chan *pb.KlineUpdate
	TradesCh    chan *pb.TradeUpdate
	OrderBookCh chan *pb.OrderBookUpdate

	// 生命周期
	cancel context.CancelFunc
	done   chan struct{}
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
	return c.client.Connect(address)
}

// Disconnect 断开连接
func (c *dataHiveClient) Disconnect() error {
	// 取消所有订阅
	c.subMutex.Lock()
	for id, sub := range c.subscriptions {
		c.closeSubscription(sub)
		delete(c.subscriptions, id)
	}
	c.subMutex.Unlock()

	return c.client.Disconnect()
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

// WatchPrice 订阅轻量级价格更新
func (c *dataHiveClient) WatchPrice(ctx context.Context, exchange, marketType, symbol string) (<-chan *pb.PriceUpdate, string, error) {
	req := &pb.SubscribeRequest{
		Exchange:   exchange,
		MarketType: marketType,
		Symbol:     symbol,
		DataType:   pb.DataType_PRICE,
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
	priceCh := make(chan *pb.PriceUpdate, 100)
	_, cancel := context.WithCancel(ctx)

	sub := &subscription{
		ID:       subscribeResp.Topic,
		Type:     "price",
		Exchange: exchange,
		Symbol:   symbol,
		PriceCh:  priceCh,
		cancel:   cancel,
		done:     make(chan struct{}),
	}

	c.subMutex.Lock()
	c.subscriptions[sub.ID] = sub
	c.subMutex.Unlock()

	return priceCh, sub.ID, nil
}

// WatchKline 订阅K线更新
func (c *dataHiveClient) WatchKline(ctx context.Context, exchange, marketType, symbol, timeframe string) (<-chan *pb.KlineUpdate, string, error) {
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
	_, cancel := context.WithCancel(ctx)

	sub := &subscription{
		ID:       subscribeResp.Topic,
		Type:     "kline",
		Exchange: exchange,
		Symbol:   symbol,
		KlineCh:  klineCh,
		cancel:   cancel,
		done:     make(chan struct{}),
	}

	c.subMutex.Lock()
	c.subscriptions[sub.ID] = sub
	c.subMutex.Unlock()

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
		Type:     "trades",
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
		Type:        "orderbook",
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

// Unwatch 取消订阅
func (c *dataHiveClient) Unwatch(subscriptionID string) error {
	c.subMutex.Lock()
	sub, exists := c.subscriptions[subscriptionID]
	if !exists {
		c.subMutex.Unlock()
		return fmt.Errorf("subscription not found: %s", subscriptionID)
	}
	delete(c.subscriptions, subscriptionID)
	c.subMutex.Unlock()

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

	c.logger.Debug("Unsubscribed", zap.String("subscription_id", subscriptionID))
	return nil
}

// =============================================================================
// Internal Message Handling
// =============================================================================

// registerHandlers 注册消息处理器
func (c *dataHiveClient) registerHandlers() {
	c.client.RegisterHandler(pb.ActionType_PRICE_UPDATE, c.handlePriceUpdate)
	c.client.RegisterHandler(pb.ActionType_KLINE_UPDATE, c.handleKlineUpdate)
	c.client.RegisterHandler(pb.ActionType_TRADE_UPDATE, c.handleTradeUpdate)
	c.client.RegisterHandler(pb.ActionType_ORDERBOOK_UPDATE, c.handleOrderBookUpdate)
}

// handlePriceUpdate 处理价格更新消息
func (c *dataHiveClient) handlePriceUpdate(msg *pb.Message) error {
	var update pb.PriceUpdate
	if err := proto.Unmarshal(msg.Data, &update); err != nil {
		return fmt.Errorf("failed to unmarshal price update: %w", err)
	}

	c.subMutex.RLock()
	sub, exists := c.subscriptions[update.Topic]
	c.subMutex.RUnlock()

	if !exists || sub.PriceCh == nil {
		return nil
	}

	select {
	case sub.PriceCh <- &update:
	default:
		c.logger.Warn("Price channel full, dropping message", zap.String("topic", update.Topic))
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
	if sub.cancel != nil {
		sub.cancel()
	}

	// 关闭channels
	if sub.PriceCh != nil {
		close(sub.PriceCh)
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
