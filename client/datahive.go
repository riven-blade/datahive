package client

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"datahive/pkg/ccxt"
	"datahive/pkg/logger"
	"datahive/pkg/protocol"

	"go.uber.org/zap"
)

type DataHiveClient interface {
	FetchMarkets(ctx context.Context, exchange string, reload ...bool) (map[string]*ccxt.Market, error)
	FetchTicker(ctx context.Context, exchange, symbol string) (*ccxt.Ticker, error)
	FetchTickers(ctx context.Context, exchange string, symbols ...string) (map[string]*ccxt.Ticker, error)
	FetchOHLCV(ctx context.Context, exchange, symbol, timeframe string, since int64, limit int32) ([]*ccxt.OHLCV, error)
	FetchTrades(ctx context.Context, exchange, symbol string, since int64, limit int32) ([]*ccxt.Trade, error)
	FetchOrderBook(ctx context.Context, exchange, symbol string, limit int32) (*ccxt.OrderBook, error)

	WatchTicker(ctx context.Context, exchange, marketType, symbol string, callback func(*ccxt.Ticker)) (string, error)
	WatchTickers(ctx context.Context, exchange, marketType string, symbols []string, callback func(map[string]*ccxt.Ticker)) (string, error)
	WatchOHLCV(ctx context.Context, exchange, marketType, symbol, timeframe string, callback func(*ccxt.OHLCV)) (string, error)
	WatchTrades(ctx context.Context, exchange, marketType, symbol string, callback func([]*ccxt.Trade)) (string, error)
	WatchOrderBook(ctx context.Context, exchange, marketType, symbol string, limit int32, callback func(*ccxt.OrderBook)) (string, error)

	Unwatch(subscriptionID string) error

	Connect(address string) error
	Disconnect() error
	IsConnected() bool
}

// exchangeClient 交易所客户端实现
type exchangeClient struct {
	client        Client
	subscriptions map[string]*subscription
	subMutex      sync.RWMutex
	connected     bool
	connMutex     sync.RWMutex
}

// subscription 订阅信息
type subscription struct {
	ID          string
	Type        string // ticker, tickers, ohlcv, trades, orderbook
	Exchange    string
	Symbol      string
	Symbols     []string
	Timeframe   string
	Limit       int32
	TickerCB    func(*ccxt.Ticker)
	TickersCB   func(map[string]*ccxt.Ticker)
	OHLCVCB     func(*ccxt.OHLCV)
	TradesCB    func([]*ccxt.Trade)
	OrderBookCB func(*ccxt.OrderBook)
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewDataHiveClient 创建新的交易所客户端
func NewDataHiveClient(client Client) DataHiveClient {
	ec := &exchangeClient{
		client:        client,
		subscriptions: make(map[string]*subscription),
	}

	// 注册推送消息处理器
	client.RegisterHandler(protocol.ActionTickerUpdate, func(msg *protocol.Message) error {
		return ec.handleTickerUpdate(msg.Payload)
	})
	client.RegisterHandler(protocol.ActionTickerUpdate, func(msg *protocol.Message) error {
		return ec.handleTickersUpdate(msg.Payload)
	})
	client.RegisterHandler(protocol.ActionKlineUpdate, func(msg *protocol.Message) error {
		return ec.handleOHLCVUpdate(msg.Payload)
	})
	client.RegisterHandler(protocol.ActionTradeUpdate, func(msg *protocol.Message) error {
		return ec.handleTradesUpdate(msg.Payload)
	})
	client.RegisterHandler(protocol.ActionOrderBookUpdate, func(msg *protocol.Message) error {
		return ec.handleOrderBookUpdate(msg.Payload)
	})

	return ec
}

// Connect 连接到服务器
func (ec *exchangeClient) Connect(address string) error {
	ctx := context.Background()

	ec.connMutex.Lock()
	defer ec.connMutex.Unlock()

	if err := ec.client.Connect(address); err != nil {
		return fmt.Errorf("failed to connect to server: %w", err)
	}

	ec.connected = true
	logger.Ctx(ctx).Info("✅ Exchange client connected to server")
	return nil
}

// Disconnect 断开连接
func (ec *exchangeClient) Disconnect() error {
	ctx := context.Background()

	ec.connMutex.Lock()
	defer ec.connMutex.Unlock()

	// 取消所有订阅
	ec.subMutex.Lock()
	for id, sub := range ec.subscriptions {
		if sub.cancel != nil {
			sub.cancel()
		}
		delete(ec.subscriptions, id)
	}
	ec.subMutex.Unlock()

	if err := ec.client.Disconnect(); err != nil {
		return err
	}

	ec.connected = false
	logger.Ctx(ctx).Info("Exchange client disconnected from server")
	return nil
}

// IsConnected 检查连接状态
func (ec *exchangeClient) IsConnected() bool {
	ec.connMutex.RLock()
	defer ec.connMutex.RUnlock()
	return ec.connected
}

// === Fetch接口实现 ===

// FetchMarkets 获取市场列表
func (ec *exchangeClient) FetchMarkets(ctx context.Context, exchange string, reload ...bool) (map[string]*ccxt.Market, error) {
	req := &protocol.FetchMarketsRequest{
		Exchange: exchange,
		// MarketType可以根据需要设置
	}

	respData, err := ec.sendRequest(ctx, protocol.ActionFetchMarkets, req)
	if err != nil {
		return nil, err
	}

	var resp protocol.FetchMarketsResponse
	if err := json.Unmarshal(respData, &resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	// 转换为map格式
	markets := make(map[string]*ccxt.Market)
	for _, market := range resp.Markets {
		ccxtMarket := &ccxt.Market{
			ID:      market.Id,
			Symbol:  market.Symbol,
			Base:    market.Base,
			Quote:   market.Quote,
			Active:  market.Active,
			Type:    market.Type,
			Spot:    market.Spot,
			Future:  market.Futures,
			Swap:    market.Swap,
			Linear:  market.Linear,
			Inverse: market.Inverse,
			Maker:   market.MakerFee,
			Taker:   market.TakerFee,
		}
		markets[market.Symbol] = ccxtMarket
	}

	return markets, nil
}

// FetchTicker 获取单个ticker
func (ec *exchangeClient) FetchTicker(ctx context.Context, exchange, symbol string) (*ccxt.Ticker, error) {
	req := &protocol.FetchTickerRequest{
		Exchange: exchange,
		Symbol:   symbol,
	}

	respData, err := ec.sendRequest(ctx, protocol.ActionFetchTicker, req)
	if err != nil {
		return nil, err
	}

	var ticker protocol.Ticker
	if err := json.Unmarshal(respData, &ticker); err != nil {
		return nil, fmt.Errorf("failed to unmarshal ticker: %w", err)
	}

	return ec.convertTicker(&ticker), nil
}

// FetchTickers 获取多个ticker
func (ec *exchangeClient) FetchTickers(ctx context.Context, exchange string, symbols ...string) (map[string]*ccxt.Ticker, error) {
	// 新协议中没有FetchTickersRequest，改用FetchTickerRequest逐个获取
	if len(symbols) == 0 {
		return nil, fmt.Errorf("symbols cannot be empty")
	}

	// 暂时只获取第一个symbol的ticker
	req := &protocol.FetchTickerRequest{
		Exchange: exchange,
		Symbol:   symbols[0],
	}

	respData, err := ec.sendRequest(ctx, protocol.ActionFetchTicker, req)
	if err != nil {
		return nil, err
	}

	var resp protocol.FetchTickerResponse
	if err := json.Unmarshal(respData, &resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	tickers := make(map[string]*ccxt.Ticker)
	// 只返回单个ticker
	tickers[resp.Ticker.Symbol] = ec.convertTicker(resp.Ticker)

	return tickers, nil
}

// FetchOHLCV 获取K线数据
func (ec *exchangeClient) FetchOHLCV(ctx context.Context, exchange, symbol, timeframe string, since int64, limit int32) ([]*ccxt.OHLCV, error) {
	req := &protocol.FetchKlinesRequest{
		Exchange:  exchange,
		Symbol:    symbol,
		Interval:  timeframe,
		StartTime: since,
		Limit:     limit,
	}

	respData, err := ec.sendRequest(ctx, protocol.ActionFetchKlines, req)
	if err != nil {
		return nil, err
	}

	var resp protocol.FetchKlinesResponse
	if err := json.Unmarshal(respData, &resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	var ohlcvs []*ccxt.OHLCV
	for _, kline := range resp.Klines {
		ohlcv := &ccxt.OHLCV{
			Timestamp: kline.OpenTime,
			Open:      kline.Open,
			High:      kline.High,
			Low:       kline.Low,
			Close:     kline.Close,
			Volume:    kline.Volume,
		}
		ohlcvs = append(ohlcvs, ohlcv)
	}

	return ohlcvs, nil
}

// FetchTrades 获取交易记录
func (ec *exchangeClient) FetchTrades(ctx context.Context, exchange, symbol string, since int64, limit int32) ([]*ccxt.Trade, error) {
	req := &protocol.FetchTradesRequest{
		Exchange: exchange,
		Symbol:   symbol,
		Since:    since,
		Limit:    limit,
	}

	respData, err := ec.sendRequest(ctx, protocol.ActionFetchTrades, req)
	if err != nil {
		return nil, err
	}

	var resp protocol.FetchTradesResponse
	if err := json.Unmarshal(respData, &resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	var trades []*ccxt.Trade
	for _, trade := range resp.Trades {
		ccxtTrade := &ccxt.Trade{
			ID:        trade.Id,
			Symbol:    trade.Symbol,
			Price:     trade.Price,
			Amount:    trade.Quantity,
			Side:      trade.Side, // 直接使用字符串
			Timestamp: trade.Timestamp,
		}
		trades = append(trades, ccxtTrade)
	}

	return trades, nil
}

// FetchOrderBook 获取订单簿
func (ec *exchangeClient) FetchOrderBook(ctx context.Context, exchange, symbol string, limit int32) (*ccxt.OrderBook, error) {
	req := &protocol.FetchOrderBookRequest{
		Exchange: exchange,
		Symbol:   symbol,
		Limit:    limit,
	}

	respData, err := ec.sendRequest(ctx, protocol.ActionFetchOrderBook, req)
	if err != nil {
		return nil, err
	}

	var resp protocol.FetchOrderBookResponse
	if err := json.Unmarshal(respData, &resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal orderbook: %w", err)
	}

	orderBook := &ccxt.OrderBook{
		Symbol:    resp.Orderbook.Symbol,
		Timestamp: resp.Orderbook.Timestamp,
		Datetime:  time.Unix(resp.Orderbook.Timestamp/1000, 0).Format(time.RFC3339),
		Bids:      ccxt.OrderBookSide{Price: make([]float64, len(resp.Orderbook.Bids)), Size: make([]float64, len(resp.Orderbook.Bids))},
		Asks:      ccxt.OrderBookSide{Price: make([]float64, len(resp.Orderbook.Asks)), Size: make([]float64, len(resp.Orderbook.Asks))},
	}

	for i, bid := range resp.Orderbook.Bids {
		orderBook.Bids.Price[i] = bid.Price
		orderBook.Bids.Size[i] = bid.Quantity
	}

	for i, ask := range resp.Orderbook.Asks {
		orderBook.Asks.Price[i] = ask.Price
		orderBook.Asks.Size[i] = ask.Quantity
	}

	return orderBook, nil
}

// === Watch接口实现 ===

// WatchTicker 订阅ticker
func (ec *exchangeClient) WatchTicker(ctx context.Context, exchange, marketType, symbol string, callback func(*ccxt.Ticker)) (string, error) {
	req := &protocol.SubscribeRequest{
		Exchange:   exchange,
		MarketType: marketType,
		Symbols:    []string{symbol},
		Ticker:     true,
	}

	subID, err := ec.sendWatchRequest(ctx, protocol.ActionSubscribe, req)
	if err != nil {
		return "", err
	}

	// 保存订阅信息
	ctx, cancel := context.WithCancel(ctx)
	sub := &subscription{
		ID:       subID,
		Type:     "ticker",
		Exchange: exchange,
		Symbol:   symbol,
		TickerCB: callback,
		ctx:      ctx,
		cancel:   cancel,
	}

	ec.subMutex.Lock()
	ec.subscriptions[subID] = sub
	ec.subMutex.Unlock()

	return subID, nil
}

// WatchTickers 订阅多个ticker
func (ec *exchangeClient) WatchTickers(ctx context.Context, exchange, marketType string, symbols []string, callback func(map[string]*ccxt.Ticker)) (string, error) {
	req := &protocol.SubscribeRequest{
		Exchange:   exchange,
		MarketType: marketType,
		Symbols:    symbols,
		Ticker:     true,
	}

	subID, err := ec.sendWatchRequest(ctx, protocol.ActionSubscribe, req)
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithCancel(ctx)
	sub := &subscription{
		ID:        subID,
		Type:      "tickers",
		Exchange:  exchange,
		Symbols:   symbols,
		TickersCB: callback,
		ctx:       ctx,
		cancel:    cancel,
	}

	ec.subMutex.Lock()
	ec.subscriptions[subID] = sub
	ec.subMutex.Unlock()

	return subID, nil
}

// WatchOHLCV 订阅K线数据
func (ec *exchangeClient) WatchOHLCV(ctx context.Context, exchange, marketType, symbol, timeframe string, callback func(*ccxt.OHLCV)) (string, error) {
	req := &protocol.SubscribeRequest{
		Exchange:      exchange,
		MarketType:    marketType,
		Symbols:       []string{symbol},
		Kline:         true,
		KlineInterval: timeframe,
	}

	subID, err := ec.sendWatchRequest(ctx, protocol.ActionSubscribe, req)
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithCancel(ctx)
	sub := &subscription{
		ID:        subID,
		Type:      "ohlcv",
		Exchange:  exchange,
		Symbol:    symbol,
		Timeframe: timeframe,
		OHLCVCB:   callback,
		ctx:       ctx,
		cancel:    cancel,
	}

	ec.subMutex.Lock()
	ec.subscriptions[subID] = sub
	ec.subMutex.Unlock()

	return subID, nil
}

// WatchTrades 订阅交易记录
func (ec *exchangeClient) WatchTrades(ctx context.Context, exchange, marketType, symbol string, callback func([]*ccxt.Trade)) (string, error) {
	req := &protocol.SubscribeRequest{
		Exchange:   exchange,
		MarketType: marketType,
		Symbols:    []string{symbol},
		Trade:      true,
	}

	subID, err := ec.sendWatchRequest(ctx, protocol.ActionSubscribe, req)
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithCancel(ctx)
	sub := &subscription{
		ID:       subID,
		Type:     "trades",
		Exchange: exchange,
		Symbol:   symbol,
		TradesCB: callback,
		ctx:      ctx,
		cancel:   cancel,
	}

	ec.subMutex.Lock()
	ec.subscriptions[subID] = sub
	ec.subMutex.Unlock()

	return subID, nil
}

// WatchOrderBook 订阅订单簿
func (ec *exchangeClient) WatchOrderBook(ctx context.Context, exchange, marketType, symbol string, limit int32, callback func(*ccxt.OrderBook)) (string, error) {
	req := &protocol.SubscribeRequest{
		Exchange:       exchange,
		MarketType:     marketType,
		Symbols:        []string{symbol},
		Orderbook:      true,
		OrderbookDepth: limit,
	}

	subID, err := ec.sendWatchRequest(ctx, protocol.ActionSubscribe, req)
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithCancel(ctx)
	sub := &subscription{
		ID:          subID,
		Type:        "orderbook",
		Exchange:    exchange,
		Symbol:      symbol,
		Limit:       limit,
		OrderBookCB: callback,
		ctx:         ctx,
		cancel:      cancel,
	}

	ec.subMutex.Lock()
	ec.subscriptions[subID] = sub
	ec.subMutex.Unlock()

	return subID, nil
}

// Unwatch 取消订阅
func (ec *exchangeClient) Unwatch(subscriptionID string) error {
	ec.subMutex.Lock()
	sub, exists := ec.subscriptions[subscriptionID]
	if exists {
		if sub.cancel != nil {
			sub.cancel()
		}
		delete(ec.subscriptions, subscriptionID)
	}
	ec.subMutex.Unlock()

	if !exists {
		return fmt.Errorf("subscription %s not found", subscriptionID)
	}

	logger.Ctx(sub.ctx).Info("Unsubscribed",
		zap.String("subscription_id", subscriptionID),
		zap.String("type", sub.Type),
		zap.String("exchange", sub.Exchange),
		zap.String("symbol", sub.Symbol))

	return nil
}

// === 内部方法 ===

// sendRequest 发送请求并等待响应
func (ec *exchangeClient) sendRequest(ctx context.Context, action protocol.ActionType, req interface{}) ([]byte, error) {
	if !ec.IsConnected() {
		return nil, fmt.Errorf("client not connected")
	}

	reqData, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := ec.client.SendRequest(action, reqData)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	// 检查响应类型，如果是错误类型则返回错误
	if resp.Type == protocol.TypeError {
		return nil, fmt.Errorf("server error: response type is error")
	}

	return resp.Payload, nil
}

// sendWatchRequest 发送订阅请求
func (ec *exchangeClient) sendWatchRequest(ctx context.Context, action protocol.ActionType, req interface{}) (string, error) {
	if !ec.IsConnected() {
		return "", fmt.Errorf("client not connected")
	}

	reqData, err := json.Marshal(req)
	if err != nil {
		return "", fmt.Errorf("failed to marshal watch request: %w", err)
	}

	resp, err := ec.client.SendRequest(action, reqData)
	if err != nil {
		return "", fmt.Errorf("failed to send watch request: %w", err)
	}

	// 检查响应类型，如果是错误类型则返回错误
	if resp.Type == protocol.TypeError {
		return "", fmt.Errorf("subscribe request failed: response type is error")
	}

	var subscribeResp protocol.SubscribeResponse
	if err := json.Unmarshal(resp.Payload, &subscribeResp); err != nil {
		return "", fmt.Errorf("failed to unmarshal subscribe response: %w", err)
	}

	if !subscribeResp.Success {
		return "", fmt.Errorf("subscribe request failed: %s", subscribeResp.Message)
	}

	// 使用Message ID作为订阅ID
	return resp.Id, nil
}

// === 消息处理方法 ===

// handleTickerUpdate 处理ticker更新
func (ec *exchangeClient) handleTickerUpdate(data []byte) error {
	var update protocol.TickerUpdate
	if err := json.Unmarshal(data, &update); err != nil {
		return err
	}

	ec.subMutex.RLock()
	sub, exists := ec.subscriptions[update.SubscriptionId]
	ec.subMutex.RUnlock()

	if exists && sub.TickerCB != nil {
		ticker := ec.convertTicker(update.Ticker)
		go sub.TickerCB(ticker)
	}

	return nil
}

// handleTickersUpdate 处理多个ticker更新
func (ec *exchangeClient) handleTickersUpdate(data []byte) error {
	var update protocol.TickerUpdate
	if err := json.Unmarshal(data, &update); err != nil {
		return err
	}

	ec.subMutex.RLock()
	sub, exists := ec.subscriptions[update.SubscriptionId]
	ec.subMutex.RUnlock()

	if exists && sub.TickersCB != nil {
		tickers := make(map[string]*ccxt.Ticker)
		// TickerUpdate只包含单个ticker，所以放到map中
		tickers[update.Ticker.Symbol] = ec.convertTicker(update.Ticker)
		go sub.TickersCB(tickers)
	}

	return nil
}

// handleOHLCVUpdate 处理K线更新
func (ec *exchangeClient) handleOHLCVUpdate(data []byte) error {
	var update protocol.KlineUpdate
	if err := json.Unmarshal(data, &update); err != nil {
		return err
	}

	ec.subMutex.RLock()
	sub, exists := ec.subscriptions[update.SubscriptionId]
	ec.subMutex.RUnlock()

	if exists && sub.OHLCVCB != nil {
		ohlcv := &ccxt.OHLCV{
			Timestamp: update.Kline.OpenTime,
			Open:      update.Kline.Open,
			High:      update.Kline.High,
			Low:       update.Kline.Low,
			Close:     update.Kline.Close,
			Volume:    update.Kline.Volume,
		}
		go sub.OHLCVCB(ohlcv)
	}

	return nil
}

// handleTradesUpdate 处理交易记录更新
func (ec *exchangeClient) handleTradesUpdate(data []byte) error {
	var update protocol.TradeUpdate
	if err := json.Unmarshal(data, &update); err != nil {
		return err
	}

	ec.subMutex.RLock()
	sub, exists := ec.subscriptions[update.SubscriptionId]
	ec.subMutex.RUnlock()

	if exists && sub.TradesCB != nil {
		var trades []*ccxt.Trade
		// TradeUpdate包含单个trade，包装成数组
		ccxtTrade := &ccxt.Trade{
			ID:        update.Trade.Id,
			Symbol:    update.Trade.Symbol,
			Price:     update.Trade.Price,
			Amount:    update.Trade.Quantity,
			Side:      update.Trade.Side, // 直接使用字符串
			Timestamp: update.Trade.Timestamp,
		}
		trades = append(trades, ccxtTrade)
		go sub.TradesCB(trades)
	}

	return nil
}

// handleOrderBookUpdate 处理订单簿更新
func (ec *exchangeClient) handleOrderBookUpdate(data []byte) error {
	var update protocol.OrderBookUpdate
	if err := json.Unmarshal(data, &update); err != nil {
		return err
	}

	ec.subMutex.RLock()
	sub, exists := ec.subscriptions[update.SubscriptionId]
	ec.subMutex.RUnlock()

	if exists && sub.OrderBookCB != nil {
		orderBook := &ccxt.OrderBook{
			Symbol:    update.Orderbook.Symbol,
			Timestamp: update.Orderbook.Timestamp,
			Datetime:  time.Unix(update.Orderbook.Timestamp/1000, 0).Format(time.RFC3339),
			Bids:      ccxt.OrderBookSide{Price: make([]float64, len(update.Orderbook.Bids)), Size: make([]float64, len(update.Orderbook.Bids))},
			Asks:      ccxt.OrderBookSide{Price: make([]float64, len(update.Orderbook.Asks)), Size: make([]float64, len(update.Orderbook.Asks))},
		}

		for i, bid := range update.Orderbook.Bids {
			orderBook.Bids.Price[i] = bid.Price
			orderBook.Bids.Size[i] = bid.Quantity
		}

		for i, ask := range update.Orderbook.Asks {
			orderBook.Asks.Price[i] = ask.Price
			orderBook.Asks.Size[i] = ask.Quantity
		}

		go sub.OrderBookCB(orderBook)
	}

	return nil
}

// convertTicker 转换ticker格式
func (ec *exchangeClient) convertTicker(ticker *protocol.Ticker) *ccxt.Ticker {
	return &ccxt.Ticker{
		Symbol:        ticker.Symbol,
		Timestamp:     ticker.Timestamp,
		Datetime:      time.Unix(ticker.Timestamp/1000, 0).Format(time.RFC3339),
		High:          ticker.High,
		Low:           ticker.Low,
		Bid:           ticker.Bid,
		BidVolume:     0, // protocol.Ticker没有这个字段
		Ask:           ticker.Ask,
		AskVolume:     0, // protocol.Ticker没有这个字段
		Vwap:          0, // protocol.Ticker没有这个字段
		Open:          ticker.Open,
		Close:         ticker.Close,
		Last:          ticker.Last,
		PreviousClose: 0, // protocol.Ticker没有这个字段
		Change:        ticker.Change,
		Percentage:    ticker.ChangePercent,
		Average:       0, // protocol.Ticker没有这个字段
		BaseVolume:    ticker.Volume,
		QuoteVolume:   0, // protocol.Ticker没有这个字段，使用Volume作为BaseVolume
	}
}
