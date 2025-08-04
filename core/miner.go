package core

import (
	"context"
	"fmt"
	"sync"
	"time"

	"datahive/pkg/ccxt"
	"datahive/pkg/logger"
	"datahive/storage"

	"go.uber.org/zap"
)

// =============================================================================
// 现代化数据矿工
// =============================================================================

// MinerStats 矿工统计
type MinerStats struct {
	Connected     bool      `json:"connected"`
	Subscriptions int       `json:"subscriptions"`
	Messages      int64     `json:"messages"`
	Errors        int64     `json:"errors"`
	LastUpdate    time.Time `json:"last_update"`
}

// Miner 现代化数据矿工实现
type Miner struct {
	// 基础信息
	exchange string
	market   string
	client   ccxt.Exchange

	// 组件
	publisher Publisher

	// 订阅管理
	subscriptions map[string]MinerSubscription
	mu            sync.RWMutex

	// 状态控制
	ctx     context.Context
	cancel  context.CancelFunc
	running bool

	// 存储
	timeSeriesStorage storage.TimeSeriesStorage
	kvStorage         storage.KVStorage

	// 统计
	stats MinerStats
}

func NewMiner(client ccxt.Exchange, exchange, market string,
	publisher Publisher, timeSeriesStorage storage.TimeSeriesStorage,
	kvStorage storage.KVStorage) *Miner {
	ctx, cancel := context.WithCancel(context.Background())

	return &Miner{
		exchange:          exchange,
		market:            market,
		client:            client,
		publisher:         publisher,
		timeSeriesStorage: timeSeriesStorage,
		kvStorage:         kvStorage,
		subscriptions:     make(map[string]MinerSubscription),
		ctx:               ctx,
		cancel:            cancel,
	}
}

// Start 启动矿工
func (m *Miner) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return fmt.Errorf("miner already running")
	}

	m.running = true
	m.stats.Connected = true
	m.stats.LastUpdate = time.Now()

	logger.Ctx(ctx).Info("Miner started",
		zap.String("exchange", m.exchange),
		zap.String("market", m.market))

	return nil
}

// Stop 停止矿工 - 实现DataMiner接口
func (m *Miner) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return nil
	}

	// 停止所有订阅
	m.cancel()
	m.running = false
	m.stats.Connected = false

	logger.Ctx(ctx).Info("Miner stopped",
		zap.String("exchange", m.exchange),
		zap.String("market", m.market))

	return nil
}

// Subscribe 订阅数据 - 实现DataMiner接口
func (m *Miner) Subscribe(req MinerSubscription) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return fmt.Errorf("miner not running")
	}

	// 生成订阅ID
	subID := m.generateSubscriptionID(req)

	// 保存订阅
	m.subscriptions[subID] = req
	m.stats.Subscriptions = len(m.subscriptions)

	// 启动数据收集 (简化版本)
	go m.collectData(req)

	logger.Ctx(m.ctx).Info("Subscription created",
		zap.String("id", subID),
		zap.String("symbol", req.Symbol),
		zap.Any("events", req.Events))

	return nil
}

// Unsubscribe 取消订阅 - 实现DataMiner接口
func (m *Miner) Unsubscribe(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.subscriptions, id)
	m.stats.Subscriptions = len(m.subscriptions)

	logger.Ctx(m.ctx).Info("Subscription removed", zap.String("id", id))
	return nil
}

// UnsubscribeBySymbolAndEvent 根据symbol和event类型取消订阅
func (m *Miner) UnsubscribeBySymbolAndEvent(symbol string, eventType EventType) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 查找匹配的订阅
	found := false
	for id, sub := range m.subscriptions {
		if sub.Symbol == symbol {
			// 检查是否包含该事件类型
			for i, event := range sub.Events {
				if event == eventType {
					// 从事件列表中移除该事件
					sub.Events = append(sub.Events[:i], sub.Events[i+1:]...)
					found = true

					// 如果没有事件了，删除整个订阅
					if len(sub.Events) == 0 {
						delete(m.subscriptions, id)
					}
					break
				}
			}
		}
	}

	if found {
		m.stats.Subscriptions = len(m.subscriptions)
		logger.Ctx(m.ctx).Info("Unsubscribed by symbol and event",
			zap.String("symbol", symbol),
			zap.String("event", string(eventType)))
	}

	return nil
}

// Health 获取健康状态
func (m *Miner) Health() Health {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status := "healthy"
	if !m.running || !m.stats.Connected {
		status = "unhealthy"
	}

	details := map[string]string{
		"exchange":      m.exchange,
		"market":        m.market,
		"running":       fmt.Sprintf("%t", m.running),
		"connected":     fmt.Sprintf("%t", m.stats.Connected),
		"subscriptions": fmt.Sprintf("%d", m.stats.Subscriptions),
		"messages":      fmt.Sprintf("%d", m.stats.Messages),
		"errors":        fmt.Sprintf("%d", m.stats.Errors),
	}

	return Health{
		Status:    status,
		Details:   details,
		Timestamp: time.Now().UnixMilli(),
	}
}

// =============================================================================
// 同步数据获取方法 - 新增实际API调用
// =============================================================================

// FetchMarkets 获取市场信息
func (m *Miner) FetchMarkets(ctx context.Context) ([]*ccxt.Market, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.running {
		return nil, fmt.Errorf("miner not running")
	}

	// 实际调用交易所API
	markets, err := m.client.FetchMarkets(ctx, map[string]interface{}{})
	if err != nil {
		m.incrementErrors()
		logger.Ctx(ctx).Error("Failed to fetch markets",
			zap.String("exchange", m.exchange),
			zap.Error(err))
		return nil, err
	}

	m.incrementMessages()
	logger.Ctx(ctx).Debug("Fetched markets successfully",
		zap.String("exchange", m.exchange),
		zap.Int("count", len(markets)))

	return markets, nil
}

// FetchTicker 获取ticker数据
func (m *Miner) FetchTicker(ctx context.Context, symbol string) (*ccxt.Ticker, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.running {
		return nil, fmt.Errorf("miner not running")
	}

	// 实际调用交易所API
	ticker, err := m.client.FetchTicker(ctx, symbol, map[string]interface{}{})
	if err != nil {
		m.incrementErrors()
		logger.Ctx(ctx).Error("Failed to fetch ticker",
			zap.String("exchange", m.exchange),
			zap.String("symbol", symbol),
			zap.Error(err))
		return nil, err
	}

	m.incrementMessages()
	logger.Ctx(ctx).Debug("Fetched ticker successfully",
		zap.String("exchange", m.exchange),
		zap.String("symbol", symbol))

	return ticker, nil
}

// FetchOHLCV 获取K线数据
func (m *Miner) FetchOHLCV(ctx context.Context, symbol, timeframe string, since int64, limit int) ([]*ccxt.OHLCV, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.running {
		return nil, fmt.Errorf("miner not running")
	}

	// 实际调用交易所API
	ohlcv, err := m.client.FetchOHLCV(ctx, symbol, timeframe, since, limit, map[string]interface{}{})
	if err != nil {
		m.incrementErrors()
		logger.Ctx(ctx).Error("Failed to fetch OHLCV",
			zap.String("exchange", m.exchange),
			zap.String("symbol", symbol),
			zap.String("timeframe", timeframe),
			zap.Error(err))
		return nil, err
	}

	m.incrementMessages()
	logger.Ctx(ctx).Debug("Fetched OHLCV successfully",
		zap.String("exchange", m.exchange),
		zap.String("symbol", symbol),
		zap.String("timeframe", timeframe),
		zap.Int("count", len(ohlcv)))

	return ohlcv, nil
}

// FetchOrderBook 获取订单簿数据
func (m *Miner) FetchOrderBook(ctx context.Context, symbol string, limit int) (*ccxt.OrderBook, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.running {
		return nil, fmt.Errorf("miner not running")
	}

	// 实际调用交易所API
	orderBook, err := m.client.FetchOrderBook(ctx, symbol, limit, map[string]interface{}{})
	if err != nil {
		m.incrementErrors()
		logger.Ctx(ctx).Error("Failed to fetch order book",
			zap.String("exchange", m.exchange),
			zap.String("symbol", symbol),
			zap.Error(err))
		return nil, err
	}

	m.incrementMessages()
	logger.Ctx(ctx).Debug("Fetched order book successfully",
		zap.String("exchange", m.exchange),
		zap.String("symbol", symbol),
		zap.Int("bids", len(orderBook.Bids.Price)),
		zap.Int("asks", len(orderBook.Asks.Price)))

	return orderBook, nil
}

// =============================================================================
// 私有方法 - 数据处理和统计 - 使用真实WebSocket连接
// =============================================================================

// collectData 数据收集 - 使用真实WebSocket连接
func (m *Miner) collectData(sub MinerSubscription) {
	logger.Ctx(m.ctx).Info("Starting WebSocket data collection",
		zap.String("exchange", m.exchange),
		zap.String("symbol", sub.Symbol),
		zap.Any("events", sub.Events))

	// 为每种事件类型启动独立的WebSocket连接
	for _, eventType := range sub.Events {
		switch eventType {
		case EventTicker:
			go m.watchTicker(sub.Symbol)
		case EventKline:
			go m.watchOHLCV(sub.Symbol, sub.Options.Interval)
		case EventTrade:
			go m.watchTrades(sub.Symbol)
		case EventOrderBook:
			go m.watchOrderBook(sub.Symbol, sub.Options.Depth)
		}
	}
}

// watchTicker 监听ticker WebSocket数据
func (m *Miner) watchTicker(symbol string) {
	ch, err := m.client.WatchTicker(m.ctx, symbol, map[string]interface{}{})
	if err != nil {
		m.incrementErrors()
		logger.Ctx(m.ctx).Error("Failed to start ticker WebSocket",
			zap.String("exchange", m.exchange),
			zap.String("symbol", symbol),
			zap.Error(err))
		return
	}

	logger.Ctx(m.ctx).Info("Started ticker WebSocket",
		zap.String("exchange", m.exchange),
		zap.String("symbol", symbol))

	for {
		select {
		case <-m.ctx.Done():
			logger.Ctx(m.ctx).Info("Stopping ticker WebSocket",
				zap.String("exchange", m.exchange),
				zap.String("symbol", symbol))
			return
		case ticker, ok := <-ch:
			if !ok {
				logger.Ctx(m.ctx).Warn("Ticker WebSocket channel closed",
					zap.String("exchange", m.exchange),
					zap.String("symbol", symbol))
				return
			}
			m.processTicker(ticker)
		}
	}
}

// watchOHLCV 监听K线 WebSocket数据
func (m *Miner) watchOHLCV(symbol, interval string) {
	if interval == "" {
		interval = "1m" // 默认1分钟
	}

	ch, err := m.client.WatchOHLCV(m.ctx, symbol, interval, 0, 0, map[string]interface{}{})
	if err != nil {
		m.incrementErrors()
		logger.Ctx(m.ctx).Error("Failed to start OHLCV WebSocket",
			zap.String("exchange", m.exchange),
			zap.String("symbol", symbol),
			zap.String("interval", interval),
			zap.Error(err))
		return
	}

	logger.Ctx(m.ctx).Info("Started OHLCV WebSocket",
		zap.String("exchange", m.exchange),
		zap.String("symbol", symbol),
		zap.String("interval", interval))

	for {
		select {
		case <-m.ctx.Done():
			logger.Ctx(m.ctx).Info("Stopping OHLCV WebSocket",
				zap.String("exchange", m.exchange),
				zap.String("symbol", symbol))
			return
		case ohlcv, ok := <-ch:
			if !ok {
				logger.Ctx(m.ctx).Warn("OHLCV WebSocket channel closed",
					zap.String("exchange", m.exchange),
					zap.String("symbol", symbol))
				return
			}
			m.processOHLCV(ohlcv, interval)
		}
	}
}

// watchTrades 监听交易 WebSocket数据
func (m *Miner) watchTrades(symbol string) {
	ch, err := m.client.WatchTrades(m.ctx, symbol, 0, 0, map[string]interface{}{})
	if err != nil {
		m.incrementErrors()
		logger.Ctx(m.ctx).Error("Failed to start trades WebSocket",
			zap.String("exchange", m.exchange),
			zap.String("symbol", symbol),
			zap.Error(err))
		return
	}

	logger.Ctx(m.ctx).Info("Started trades WebSocket",
		zap.String("exchange", m.exchange),
		zap.String("symbol", symbol))

	for {
		select {
		case <-m.ctx.Done():
			logger.Ctx(m.ctx).Info("Stopping trades WebSocket",
				zap.String("exchange", m.exchange),
				zap.String("symbol", symbol))
			return
		case trade, ok := <-ch:
			if !ok {
				logger.Ctx(m.ctx).Warn("Trades WebSocket channel closed",
					zap.String("exchange", m.exchange),
					zap.String("symbol", symbol))
				return
			}
			m.processTrade(trade)
		}
	}
}

// watchOrderBook 监听订单簿 WebSocket数据
func (m *Miner) watchOrderBook(symbol string, depth int) {
	if depth <= 0 {
		depth = 20 // 默认20档
	}

	ch, err := m.client.WatchOrderBook(m.ctx, symbol, depth, map[string]interface{}{})
	if err != nil {
		m.incrementErrors()
		logger.Ctx(m.ctx).Error("Failed to start orderbook WebSocket",
			zap.String("exchange", m.exchange),
			zap.String("symbol", symbol),
			zap.Int("depth", depth),
			zap.Error(err))
		return
	}

	logger.Ctx(m.ctx).Info("Started orderbook WebSocket",
		zap.String("exchange", m.exchange),
		zap.String("symbol", symbol),
		zap.Int("depth", depth))

	for {
		select {
		case <-m.ctx.Done():
			logger.Ctx(m.ctx).Info("Stopping orderbook WebSocket",
				zap.String("exchange", m.exchange),
				zap.String("symbol", symbol))
			return
		case orderBook, ok := <-ch:
			if !ok {
				logger.Ctx(m.ctx).Warn("OrderBook WebSocket channel closed",
					zap.String("exchange", m.exchange),
					zap.String("symbol", symbol))
				return
			}
			m.processOrderBook(orderBook)
		}
	}
}

// processSubscription 处理订阅 - 简化版本 (已被上面的WebSocket方法替代)
func (m *Miner) processSubscription(sub MinerSubscription) {
	// 这个方法现在已经不需要了，因为我们使用真实的WebSocket连接
	// 保留以便向后兼容或作为fallback
	logger.Ctx(m.ctx).Debug("processSubscription called (legacy method)")
}

// processTicker 处理ticker数据 - 使用真实WebSocket数据
func (m *Miner) processTicker(watchTicker *ccxt.WatchTicker) {
	// 转换为标准格式的数据结构
	tickerData := map[string]interface{}{
		"symbol":     watchTicker.Symbol,
		"timestamp":  watchTicker.TimeStamp,
		"high":       watchTicker.High,
		"low":        watchTicker.Low,
		"bid":        watchTicker.Bid,
		"ask":        watchTicker.Ask,
		"open":       watchTicker.Open,
		"close":      watchTicker.Close,
		"last":       watchTicker.Last,
		"volume":     watchTicker.BaseVolume,
		"change":     watchTicker.Change,
		"percentage": watchTicker.Percentage,
		"channel":    watchTicker.Channel,
	}

	event := Event{
		Type:      EventTicker,
		Source:    m.exchange,
		Market:    m.market,
		Symbol:    watchTicker.Symbol,
		Data:      tickerData, // 使用转换后的数据
		Timestamp: time.Now().UnixMilli(),
	}

	// 发布事件
	if err := m.publisher.Publish(m.ctx, event); err != nil {
		m.incrementErrors()
		logger.Ctx(m.ctx).Error("Failed to publish ticker", zap.Error(err))
		return
	}

	// 保存到存储
	dataPoint := DataPoint{
		Type:      event.Type,
		Exchange:  event.Source,
		Market:    event.Market,
		Symbol:    event.Symbol,
		Data:      event.Data,
		Timestamp: event.Timestamp,
	}

	if err := m.storage.Save(m.ctx, dataPoint); err != nil {
		m.incrementErrors()
		logger.Ctx(m.ctx).Error("Failed to save ticker", zap.Error(err))
		return
	}

	m.incrementMessages()
	logger.Ctx(m.ctx).Debug("Processed ticker data",
		zap.String("symbol", watchTicker.Symbol),
		zap.Float64("price", watchTicker.Last))
}

// processOHLCV 处理K线数据 - 使用真实WebSocket数据
func (m *Miner) processOHLCV(watchOHLCV *ccxt.WatchOHLCV, interval string) {
	// 转换为标准格式的数据结构
	ohlcvData := map[string]interface{}{
		"symbol":    watchOHLCV.Symbol,
		"timeframe": watchOHLCV.Timeframe,
		"timestamp": watchOHLCV.Timestamp,
		"open":      watchOHLCV.Open,
		"high":      watchOHLCV.High,
		"low":       watchOHLCV.Low,
		"close":     watchOHLCV.Close,
		"volume":    watchOHLCV.Volume,
		"channel":   watchOHLCV.Channel,
	}

	event := Event{
		Type:      EventKline,
		Source:    m.exchange,
		Market:    m.market,
		Symbol:    watchOHLCV.Symbol,
		Data:      ohlcvData, // 使用转换后的数据
		Timestamp: time.Now().UnixMilli(),
	}

	// 发布事件
	if err := m.publisher.Publish(m.ctx, event); err != nil {
		m.incrementErrors()
		logger.Ctx(m.ctx).Error("Failed to publish OHLCV", zap.Error(err))
		return
	}

	// 保存到存储
	dataPoint := DataPoint{
		Type:      event.Type,
		Exchange:  event.Source,
		Market:    event.Market,
		Symbol:    event.Symbol,
		Data:      event.Data,
		Timestamp: event.Timestamp,
	}

	if err := m.storage.Save(m.ctx, dataPoint); err != nil {
		m.incrementErrors()
		logger.Ctx(m.ctx).Error("Failed to save OHLCV", zap.Error(err))
		return
	}

	m.incrementMessages()
	logger.Ctx(m.ctx).Debug("Processed OHLCV data",
		zap.String("symbol", watchOHLCV.Symbol),
		zap.String("interval", interval))
}

// processTrade 处理交易数据 - 使用真实WebSocket数据
func (m *Miner) processTrade(watchTrade *ccxt.WatchTrade) {
	// 转换为标准格式的数据结构
	tradeData := map[string]interface{}{
		"symbol":       watchTrade.Symbol,
		"id":           watchTrade.ID,
		"timestamp":    watchTrade.Timestamp,
		"price":        watchTrade.Price,
		"amount":       watchTrade.Amount,
		"cost":         watchTrade.Cost,
		"side":         watchTrade.Side,
		"type":         watchTrade.Type,
		"takerOrMaker": watchTrade.TakerOrMaker,
		"channel":      watchTrade.Channel,
	}

	event := Event{
		Type:      EventTrade,
		Source:    m.exchange,
		Market:    m.market,
		Symbol:    watchTrade.Symbol,
		Data:      tradeData, // 使用转换后的数据
		Timestamp: time.Now().UnixMilli(),
	}

	// 发布事件
	if err := m.publisher.Publish(m.ctx, event); err != nil {
		m.incrementErrors()
		logger.Ctx(m.ctx).Error("Failed to publish trade", zap.Error(err))
		return
	}

	// 保存到存储
	dataPoint := DataPoint{
		Type:      event.Type,
		Exchange:  event.Source,
		Market:    event.Market,
		Symbol:    event.Symbol,
		Data:      event.Data,
		Timestamp: event.Timestamp,
	}

	if err := m.storage.Save(m.ctx, dataPoint); err != nil {
		m.incrementErrors()
		logger.Ctx(m.ctx).Error("Failed to save trade", zap.Error(err))
		return
	}

	m.incrementMessages()
	logger.Ctx(m.ctx).Debug("Processed trade data",
		zap.String("symbol", watchTrade.Symbol),
		zap.Float64("price", watchTrade.Price),
		zap.Float64("amount", watchTrade.Amount))
}

// processOrderBook 处理订单簿数据 - 使用真实WebSocket数据
func (m *Miner) processOrderBook(watchOrderBook *ccxt.WatchOrderBook) {
	// 转换为标准格式的数据结构
	orderBookData := map[string]interface{}{
		"symbol":    watchOrderBook.Symbol,
		"timestamp": watchOrderBook.TimeStamp,
		"bids":      watchOrderBook.Bids,
		"asks":      watchOrderBook.Asks,
		"nonce":     watchOrderBook.Nonce,
		"channel":   watchOrderBook.Channel,
	}

	event := Event{
		Type:      EventOrderBook,
		Source:    m.exchange,
		Market:    m.market,
		Symbol:    watchOrderBook.Symbol,
		Data:      orderBookData, // 使用转换后的数据
		Timestamp: time.Now().UnixMilli(),
	}

	// 发布事件
	if err := m.publisher.Publish(m.ctx, event); err != nil {
		m.incrementErrors()
		logger.Ctx(m.ctx).Error("Failed to publish orderbook", zap.Error(err))
		return
	}

	// 保存到存储
	dataPoint := DataPoint{
		Type:      event.Type,
		Exchange:  event.Source,
		Market:    event.Market,
		Symbol:    event.Symbol,
		Data:      event.Data,
		Timestamp: event.Timestamp,
	}

	if err := m.storage.Save(m.ctx, dataPoint); err != nil {
		m.incrementErrors()
		logger.Ctx(m.ctx).Error("Failed to save orderbook", zap.Error(err))
		return
	}

	m.incrementMessages()
	logger.Ctx(m.ctx).Debug("Processed orderbook data",
		zap.String("symbol", watchOrderBook.Symbol))
}

// generateSubscriptionID 生成订阅ID
func (m *Miner) generateSubscriptionID(req MinerSubscription) string {
	return fmt.Sprintf("%s_%s_%s_%d",
		m.exchange,
		m.market,
		req.Symbol,
		time.Now().UnixNano())
}

// incrementMessages 增加消息计数
func (m *Miner) incrementMessages() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stats.Messages++
	m.stats.LastUpdate = time.Now()
}

// incrementErrors 增加错误计数
func (m *Miner) incrementErrors() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stats.Errors++
	m.stats.LastUpdate = time.Now()
}
