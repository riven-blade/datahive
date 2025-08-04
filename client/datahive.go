package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	"datahive/pkg/ccxt"
	"datahive/pkg/logger"
	"datahive/pkg/protocol/pb"

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
	FetchMarkets(ctx context.Context, exchange, marketType, stackType string, reload ...bool) (map[string]*ccxt.Market, error)
	FetchTicker(ctx context.Context, exchange, marketType, symbol string) (*ccxt.Ticker, error)
	FetchTickers(ctx context.Context, exchange, marketType string, symbols ...string) (map[string]*ccxt.Ticker, error)
	FetchOHLCV(ctx context.Context, exchange, marketType, symbol, timeframe string, since int64, limit int32) ([]*ccxt.OHLCV, error)
	FetchTrades(ctx context.Context, exchange, marketType, symbol string, since int64, limit int32) ([]*ccxt.Trade, error)
	FetchOrderBook(ctx context.Context, exchange, marketType, symbol string, limit int32) (*ccxt.OrderBook, error)

	// 实时订阅 (WebSocket)
	WatchPrice(ctx context.Context, exchange, marketType, symbol string) (<-chan *ccxt.WatchPrice, string, error)
	WatchOHLCV(ctx context.Context, exchange, marketType, symbol, timeframe string) (<-chan *ccxt.OHLCV, string, error)
	WatchTrades(ctx context.Context, exchange, marketType, symbol string) (<-chan *ccxt.Trade, string, error)
	WatchOrderBook(ctx context.Context, exchange, marketType, symbol string, limit int32) (<-chan *ccxt.OrderBook, string, error)
	Unwatch(subscriptionID string) error

	// 状态和错误处理
	SetErrorHandler(handler func(error))
	GetStats() *ClientStats
}

// subscription 订阅信息
type subscription struct {
	ID       string
	Type     string // price, ohlcv, trades, orderbook
	Exchange string
	Symbol   string

	// Channels
	PriceCh     chan *ccxt.WatchPrice
	OHLCVCh     chan *ccxt.OHLCV
	TradesCh    chan *ccxt.Trade
	OrderBookCh chan *ccxt.OrderBook

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
func (c *dataHiveClient) FetchMarkets(ctx context.Context, exchange, marketType, stackType string, reload ...bool) (map[string]*ccxt.Market, error) {
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

	markets := make(map[string]*ccxt.Market)
	for _, market := range marketsResp.Markets {
		markets[market.Symbol] = c.convertMarket(market)
	}

	return markets, nil
}

// FetchTicker 获取单个ticker
func (c *dataHiveClient) FetchTicker(ctx context.Context, exchange, marketType, symbol string) (*ccxt.Ticker, error) {
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

	return c.convertTicker(tickerResp.Ticker), nil
}

// FetchTickers 获取多个tickers
func (c *dataHiveClient) FetchTickers(ctx context.Context, exchange, marketType string, symbols ...string) (map[string]*ccxt.Ticker, error) {
	if len(symbols) == 0 {
		return nil, fmt.Errorf("at least one symbol is required")
	}

	tickers := make(map[string]*ccxt.Ticker)

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

		tickers[tickersResp.Ticker.Symbol] = c.convertTicker(tickersResp.Ticker)
	}

	return tickers, nil
}

// FetchOHLCV 获取K线数据
func (c *dataHiveClient) FetchOHLCV(ctx context.Context, exchange, marketType, symbol, timeframe string, since int64, limit int32) ([]*ccxt.OHLCV, error) {
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

	var ohlcvs []*ccxt.OHLCV
	for _, kline := range klinesResp.Klines {
		ohlcvs = append(ohlcvs, c.convertKlineToOHLCV(kline))
	}

	return ohlcvs, nil
}

// FetchTrades 获取交易数据
func (c *dataHiveClient) FetchTrades(ctx context.Context, exchange, marketType, symbol string, since int64, limit int32) ([]*ccxt.Trade, error) {
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

	var trades []*ccxt.Trade
	for _, trade := range tradesResp.Trades {
		trades = append(trades, c.convertTrade(trade))
	}

	return trades, nil
}

// FetchOrderBook 获取订单簿
func (c *dataHiveClient) FetchOrderBook(ctx context.Context, exchange, marketType, symbol string, limit int32) (*ccxt.OrderBook, error) {
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

	return c.convertOrderBook(orderBookResp.Orderbook), nil
}

// =============================================================================
// Watch Methods (Real-time WebSocket)
// =============================================================================

// WatchPrice 订阅价格更新
func (c *dataHiveClient) WatchPrice(ctx context.Context, exchange, marketType, symbol string) (<-chan *ccxt.WatchPrice, string, error) {
	req := &pb.SubscribeRequest{
		Exchange:   exchange,
		MarketType: marketType,
		Symbol:     symbol,
		DataType:   pb.DataType_TICKER,
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
	priceCh := make(chan *ccxt.WatchPrice, 100)
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

// WatchOHLCV 订阅K线更新
func (c *dataHiveClient) WatchOHLCV(ctx context.Context, exchange, marketType, symbol, timeframe string) (<-chan *ccxt.OHLCV, string, error) {
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
	ohlcvCh := make(chan *ccxt.OHLCV, 100)
	_, cancel := context.WithCancel(ctx)

	sub := &subscription{
		ID:       subscribeResp.Topic,
		Type:     "ohlcv",
		Exchange: exchange,
		Symbol:   symbol,
		OHLCVCh:  ohlcvCh,
		cancel:   cancel,
		done:     make(chan struct{}),
	}

	c.subMutex.Lock()
	c.subscriptions[sub.ID] = sub
	c.subMutex.Unlock()

	return ohlcvCh, sub.ID, nil
}

// WatchTrades 订阅交易更新
func (c *dataHiveClient) WatchTrades(ctx context.Context, exchange, marketType, symbol string) (<-chan *ccxt.Trade, string, error) {
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
	tradesCh := make(chan *ccxt.Trade, 100)
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
func (c *dataHiveClient) WatchOrderBook(ctx context.Context, exchange, marketType, symbol string, limit int32) (<-chan *ccxt.OrderBook, string, error) {
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
	orderBookCh := make(chan *ccxt.OrderBook, 100)
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
	c.client.RegisterHandler(pb.ActionType_TICKER_UPDATE, c.handleTickerUpdate)
	c.client.RegisterHandler(pb.ActionType_KLINE_UPDATE, c.handleKlineUpdate)
	c.client.RegisterHandler(pb.ActionType_TRADE_UPDATE, c.handleTradeUpdate)
	c.client.RegisterHandler(pb.ActionType_ORDERBOOK_UPDATE, c.handleOrderBookUpdate)
}

// handleTickerUpdate 处理ticker更新消息
func (c *dataHiveClient) handleTickerUpdate(msg *pb.Message) error {
	var update pb.TickerUpdate
	if err := proto.Unmarshal(msg.Data, &update); err != nil {
		return fmt.Errorf("failed to unmarshal ticker update: %w", err)
	}

	c.subMutex.RLock()
	sub, exists := c.subscriptions[update.Topic]
	c.subMutex.RUnlock()

	if !exists || sub.PriceCh == nil {
		return nil
	}

	// 转换为WatchPrice格式
	watchPrice := &ccxt.WatchPrice{
		Symbol:     update.Ticker.Symbol,
		Price:      update.Ticker.Last,
		TimeStamp:  update.Ticker.Timestamp,
		StreamName: update.Topic,
	}

	select {
	case sub.PriceCh <- watchPrice:
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

	if !exists || sub.OHLCVCh == nil {
		return nil
	}

	ohlcv := c.convertKlineToOHLCV(update.Kline)

	select {
	case sub.OHLCVCh <- ohlcv:
	default:
		c.logger.Warn("OHLCV channel full, dropping message", zap.String("topic", update.Topic))
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

	trade := c.convertTrade(update.Trade)

	select {
	case sub.TradesCh <- trade:
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

	orderBook := c.convertOrderBook(update.Orderbook)

	select {
	case sub.OrderBookCh <- orderBook:
	default:
		c.logger.Warn("OrderBook channel full, dropping message", zap.String("topic", update.Topic))
	}

	return nil
}

// =============================================================================
// Data Conversion Functions
// =============================================================================

// convertTicker 转换ticker数据
func (c *dataHiveClient) convertTicker(ticker *pb.Ticker) *ccxt.Ticker {
	return &ccxt.Ticker{
		Symbol:     ticker.Symbol,
		TimeStamp:  ticker.Timestamp,
		High:       ticker.High,
		Low:        ticker.Low,
		Bid:        ticker.Bid,
		Ask:        ticker.Ask,
		Last:       ticker.Last,
		Open:       ticker.Open,
		Close:      ticker.Close,
		BaseVolume: ticker.Volume,
		Change:     ticker.Change,
		Percentage: ticker.ChangePercent,
	}
}

// convertMarket 转换市场数据
func (c *dataHiveClient) convertMarket(market *pb.Market) *ccxt.Market {
	return &ccxt.Market{
		ID:      market.Id,
		Symbol:  market.Symbol,
		Base:    market.Base,
		Quote:   market.Quote,
		Active:  market.Active,
		Type:    market.Type,
		Spot:    market.Spot,
		Linear:  market.Linear,
		Inverse: market.Inverse,
	}
}

// convertKlineToOHLCV 转换K线数据为OHLCV
func (c *dataHiveClient) convertKlineToOHLCV(kline *pb.Kline) *ccxt.OHLCV {
	return &ccxt.OHLCV{
		Timestamp: kline.OpenTime,
		Open:      kline.Open,
		High:      kline.High,
		Low:       kline.Low,
		Close:     kline.Close,
		Volume:    kline.Volume,
	}
}

// convertTrade 转换交易数据
func (c *dataHiveClient) convertTrade(trade *pb.Trade) *ccxt.Trade {
	return &ccxt.Trade{
		ID:        trade.Id,
		Symbol:    trade.Symbol,
		Price:     trade.Price,
		Amount:    trade.Quantity,
		Side:      trade.Side,
		Timestamp: trade.Timestamp,
	}
}

// convertOrderBook 转换订单簿数据
func (c *dataHiveClient) convertOrderBook(orderbook *pb.OrderBook) *ccxt.OrderBook {
	var bids ccxt.OrderBookSide
	var asks ccxt.OrderBookSide

	// 转换bids
	if len(orderbook.Bids) > 0 {
		bidPrices := make([]float64, len(orderbook.Bids))
		bidSizes := make([]float64, len(orderbook.Bids))
		for i, bid := range orderbook.Bids {
			bidPrices[i] = bid.Price
			bidSizes[i] = bid.Quantity
		}
		bids = ccxt.OrderBookSide{
			Price: bidPrices,
			Size:  bidSizes,
		}
	}

	// 转换asks
	if len(orderbook.Asks) > 0 {
		askPrices := make([]float64, len(orderbook.Asks))
		askSizes := make([]float64, len(orderbook.Asks))
		for i, ask := range orderbook.Asks {
			askPrices[i] = ask.Price
			askSizes[i] = ask.Quantity
		}
		asks = ccxt.OrderBookSide{
			Price: askPrices,
			Size:  askSizes,
		}
	}

	return &ccxt.OrderBook{
		Symbol:    orderbook.Symbol,
		Bids:      bids,
		Asks:      asks,
		TimeStamp: orderbook.Timestamp,
		Nonce:     0, // 如果protocol中有nonce字段可以添加
	}
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
	if sub.OHLCVCh != nil {
		close(sub.OHLCVCh)
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
