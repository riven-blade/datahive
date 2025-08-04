package core

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/riven-blade/datahive/pkg/ccxt"
	"github.com/riven-blade/datahive/pkg/logger"
	"github.com/riven-blade/datahive/pkg/protocol/pb"
	storagelib "github.com/riven-blade/datahive/storage"

	"github.com/spf13/cast"
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
	subscriptions map[string]*MinerSubscription
	mu            sync.RWMutex

	// 状态控制
	ctx     context.Context
	cancel  context.CancelFunc
	running bool

	// 存储
	storage *storagelib.StorageManager

	// 类型转换器
	typeConverter *storagelib.TypeConverter

	// 统计
	stats MinerStats
}

func NewMiner(client ccxt.Exchange, exchange, market string,
	publisher Publisher, storage *storagelib.StorageManager) *Miner {
	ctx, cancel := context.WithCancel(context.Background())

	return &Miner{
		exchange:      exchange,
		market:        market,
		client:        client,
		publisher:     publisher,
		storage:       storage,
		typeConverter: storagelib.NewTypeConverter(),
		subscriptions: make(map[string]*MinerSubscription),
		ctx:           ctx,
		cancel:        cancel,
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

// Subscribe 订阅数据
func (m *Miner) Subscribe(req *MinerSubscription) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return fmt.Errorf("miner not running")
	}

	// 生成Channel
	req.StreamName = m.client.GenerateChannel(req.Symbol, req.GetChannelParams())
	// 检查是否已经订阅了该streamName
	if _, exists := m.subscriptions[req.StreamName]; exists {
		logger.Ctx(m.ctx).Debug("channel already subscribed",
			zap.String("channel", req.StreamName))
		return nil
	}

	// 保存订阅
	m.subscriptions[req.StreamName] = req
	m.stats.Subscriptions = len(m.subscriptions)

	// 向交易所提交订阅请求
	go m.subscribeAction(req)

	logger.Ctx(m.ctx).Info("Subscription created",
		zap.String("symbol", req.Symbol),
		zap.String("event", string(req.Event)))

	return nil
}

// Unsubscribe
func (m *Miner) Unsubscribe(topic string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.subscriptions[topic]; !exists {
		logger.Ctx(m.ctx).Debug("Topic not found for unsubscription",
			zap.String("topic", topic))
		return nil // 不存在就算成功
	}

	delete(m.subscriptions, topic)
	m.stats.Subscriptions = len(m.subscriptions)

	logger.Ctx(m.ctx).Info("Subscription removed", zap.String("topic", topic))
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
	logger.Ctx(ctx).Debug("Miner.FetchMarkets开始执行",
		zap.String("exchange", m.exchange),
		zap.String("market", m.market))

	m.mu.RLock()
	running := m.running
	client := m.client
	m.mu.RUnlock()

	logger.Ctx(ctx).Debug("检查Miner运行状态", zap.Bool("running", running))
	if !running {
		logger.Ctx(ctx).Error("Miner未运行状态",
			zap.String("exchange", m.exchange),
			zap.String("market", m.market))
		return nil, fmt.Errorf("miner not running")
	}

	logger.Ctx(ctx).Debug("开始调用交易所API")
	markets, err := client.FetchMarkets(ctx, map[string]interface{}{})
	if err != nil {
		logger.Ctx(ctx).Error("Failed to fetch markets",
			zap.String("exchange", m.exchange),
			zap.Error(err))
		return nil, err
	}

	logger.Ctx(ctx).Debug("Fetched markets successfully",
		zap.String("exchange", m.exchange),
		zap.Int("count", len(markets)))

	return markets, nil
}

// FetchTicker 获取ticker数据
func (m *Miner) FetchTicker(ctx context.Context, symbol string) (*ccxt.Ticker, error) {
	// 只在必要时加锁，减少锁的持有时间
	m.mu.RLock()
	running := m.running
	client := m.client
	m.mu.RUnlock()

	if !running {
		return nil, fmt.Errorf("miner not running")
	}

	// 在锁外调用API，避免阻塞其他操作
	ticker, err := client.FetchTicker(ctx, symbol, map[string]interface{}{})
	if err != nil {
		logger.Ctx(ctx).Error("Failed to fetch ticker",
			zap.String("exchange", m.exchange),
			zap.String("symbol", symbol),
			zap.Error(err))
		return nil, err
	}

	logger.Ctx(ctx).Debug("Fetched ticker successfully",
		zap.String("exchange", m.exchange),
		zap.String("symbol", symbol))

	return ticker, nil
}

// FetchOHLCV 获取K线数据
func (m *Miner) FetchOHLCV(ctx context.Context, symbol, timeframe string, since int64, limit int) ([]*ccxt.OHLCV, error) {
	// 只在必要时加锁，减少锁的持有时间
	m.mu.RLock()
	running := m.running
	client := m.client
	m.mu.RUnlock()

	if !running {
		return nil, fmt.Errorf("miner not running")
	}

	// 在锁外调用API，避免阻塞其他操作
	ohlcv, err := client.FetchOHLCV(ctx, symbol, timeframe, since, limit, map[string]interface{}{})
	if err != nil {
		logger.Ctx(ctx).Error("Failed to fetch OHLCV",
			zap.String("exchange", m.exchange),
			zap.String("symbol", symbol),
			zap.String("timeframe", timeframe),
			zap.Error(err))
		return nil, err
	}

	logger.Ctx(ctx).Debug("Fetched OHLCV successfully",
		zap.String("exchange", m.exchange),
		zap.String("symbol", symbol),
		zap.String("timeframe", timeframe),
		zap.Int("count", len(ohlcv)))

	return ohlcv, nil
}

// FetchOrderBook 获取订单簿数据
func (m *Miner) FetchOrderBook(ctx context.Context, symbol string, limit int) (*ccxt.OrderBook, error) {
	// 只在必要时加锁，减少锁的持有时间
	m.mu.RLock()
	running := m.running
	client := m.client
	m.mu.RUnlock()

	if !running {
		return nil, fmt.Errorf("miner not running")
	}

	// 在锁外调用API，避免阻塞其他操作
	orderBook, err := client.FetchOrderBook(ctx, symbol, limit, map[string]interface{}{})
	if err != nil {
		logger.Ctx(ctx).Error("Failed to fetch order book",
			zap.String("exchange", m.exchange),
			zap.String("symbol", symbol),
			zap.Error(err))
		return nil, err
	}

	logger.Ctx(ctx).Debug("Fetched order book successfully",
		zap.String("exchange", m.exchange),
		zap.String("symbol", symbol),
		zap.Int("bids", len(orderBook.Bids.Price)),
		zap.Int("asks", len(orderBook.Asks.Price)))

	return orderBook, nil
}

// FetchTrades 获取交易记录
func (m *Miner) FetchTrades(ctx context.Context, symbol string, since int64, limit int) ([]*ccxt.Trade, error) {
	// 只在必要时加锁，减少锁的持有时间
	m.mu.RLock()
	running := m.running
	client := m.client
	m.mu.RUnlock()

	if !running {
		return nil, fmt.Errorf("miner not running")
	}

	// 在锁外调用API，避免阻塞其他操作
	trades, err := client.FetchTrades(ctx, symbol, since, limit, map[string]interface{}{})
	if err != nil {
		logger.Ctx(ctx).Error("Failed to fetch trades",
			zap.String("exchange", m.exchange),
			zap.String("symbol", symbol),
			zap.Error(err))
		return nil, err
	}

	logger.Ctx(ctx).Debug("Fetched trades successfully",
		zap.String("exchange", m.exchange),
		zap.String("symbol", symbol),
		zap.Int("count", len(trades)))

	return trades, nil
}

// subscribeAction 开启订阅
func (m *Miner) subscribeAction(sub *MinerSubscription) {
	logger.Ctx(m.ctx).Debug("Starting WebSocket subscription with dedicated channel",
		zap.String("exchange", m.exchange),
		zap.String("event", string(sub.Event)),
		zap.String("topic", sub.Topic))

	// 根据事件类型调用相应的Watch方法，获取专用channel
	var subscriptionID string
	var err error
	params := sub.ToMap()

	// 创建用于取消该订阅的context
	subCtx, cancel := context.WithCancel(m.ctx)
	sub.CancelFunc = cancel

	switch sub.Event {
	case EventPrice:
		var priceChan <-chan *ccxt.WatchPrice
		subscriptionID, priceChan, err = m.client.WatchPrice(subCtx, sub.Symbol, params)
		if err == nil {
			sub.DataChannel = priceChan
			go m.processPriceData(subCtx, sub, priceChan)
		}
	case EventKline:
		timeframe := sub.Interval
		if timeframe == "" {
			timeframe = "1m"
		}
		var klineChan <-chan *ccxt.WatchOHLCV
		subscriptionID, klineChan, err = m.client.WatchOHLCV(subCtx, sub.Symbol, timeframe, params)
		if err == nil {
			sub.DataChannel = klineChan
			go m.processKlineData(subCtx, sub, klineChan)
		}
	case EventTrade:
		var tradeChan <-chan *ccxt.WatchTrade
		subscriptionID, tradeChan, err = m.client.WatchTrade(subCtx, sub.Symbol, params)
		if err == nil {
			sub.DataChannel = tradeChan
			go m.processTradeData(subCtx, sub, tradeChan)
		}
	case EventOrderBook:
		// 将深度信息添加到参数中
		if sub.Depth > 0 {
			if params == nil {
				params = make(map[string]interface{})
			}
			params["limit"] = sub.Depth
		}
		var orderBookChan <-chan *ccxt.WatchOrderBook
		subscriptionID, orderBookChan, err = m.client.WatchOrderBook(subCtx, sub.Symbol, params)
		if err == nil {
			sub.DataChannel = orderBookChan
			go m.processOrderBookData(subCtx, sub, orderBookChan)
		}
	default:
		err = fmt.Errorf("unsupported event type: %s", sub.Event)
	}

	if err != nil {
		cancel() // 取消context
		m.incrementErrors()
		logger.Ctx(m.ctx).Error("Failed to start WebSocket subscription",
			zap.String("exchange", m.exchange),
			zap.String("symbol", sub.Symbol),
			zap.String("event", string(sub.Event)),
			zap.Error(err))
		return
	}

	// 保存订阅ID
	sub.SubscriptionID = subscriptionID

	logger.Ctx(m.ctx).Info("Started WebSocket subscription with dedicated channel",
		zap.String("exchange", m.exchange),
		zap.String("symbol", sub.Symbol),
		zap.String("event", string(sub.Event)),
		zap.String("subscription_id", subscriptionID),
		zap.String("StreamName", sub.StreamName))
}

// 新架构：专用channel数据处理方法

// processPriceData 处理价格专用channel数据 (WatchPrice)
func (m *Miner) processPriceData(ctx context.Context, sub *MinerSubscription, priceChan <-chan *ccxt.WatchPrice) {
	logger.Ctx(ctx).Info("Starting price data processor",
		zap.String("symbol", sub.Symbol),
		zap.String("subscription_id", sub.SubscriptionID))

	for {
		select {
		case <-ctx.Done():
			logger.Ctx(ctx).Info("Stopping price data processor",
				zap.String("symbol", sub.Symbol))
			return
		case price, ok := <-priceChan:
			if !ok {
				logger.Ctx(ctx).Warn("Price channel closed",
					zap.String("symbol", sub.Symbol))
				return
			}
			m.processPrice(price)
		}
	}
}

// processKlineData 处理kline专用channel数据
func (m *Miner) processKlineData(ctx context.Context, sub *MinerSubscription, klineChan <-chan *ccxt.WatchOHLCV) {
	logger.Ctx(ctx).Info("Starting kline data processor",
		zap.String("symbol", sub.Symbol),
		zap.String("interval", sub.Interval),
		zap.String("subscription_id", sub.SubscriptionID))

	for {
		select {
		case <-ctx.Done():
			logger.Ctx(ctx).Info("Stopping kline data processor",
				zap.String("symbol", sub.Symbol))
			return
		case kline, ok := <-klineChan:
			if !ok {
				logger.Ctx(ctx).Warn("Kline channel closed",
					zap.String("symbol", sub.Symbol))
				return
			}
			m.processOHLCV(kline) // 复用原有的处理逻辑
		}
	}
}

// processTradeData 处理trade专用channel数据
func (m *Miner) processTradeData(ctx context.Context, sub *MinerSubscription, tradeChan <-chan *ccxt.WatchTrade) {
	logger.Ctx(ctx).Info("Starting trade data processor",
		zap.String("symbol", sub.Symbol),
		zap.String("subscription_id", sub.SubscriptionID))

	for {
		select {
		case <-ctx.Done():
			logger.Ctx(ctx).Info("Stopping trade data processor",
				zap.String("symbol", sub.Symbol))
			return
		case trade, ok := <-tradeChan:
			if !ok {
				logger.Ctx(ctx).Warn("Trade channel closed",
					zap.String("symbol", sub.Symbol))
				return
			}
			m.processTrade(trade) // 复用原有的处理逻辑
		}
	}
}

// processOrderBookData 处理orderbook专用channel数据
func (m *Miner) processOrderBookData(ctx context.Context, sub *MinerSubscription, orderBookChan <-chan *ccxt.WatchOrderBook) {
	logger.Ctx(ctx).Info("Starting orderbook data processor",
		zap.String("symbol", sub.Symbol),
		zap.String("subscription_id", sub.SubscriptionID))

	for {
		select {
		case <-ctx.Done():
			logger.Ctx(ctx).Info("Stopping orderbook data processor",
				zap.String("symbol", sub.Symbol))
			return
		case orderBook, ok := <-orderBookChan:
			if !ok {
				logger.Ctx(ctx).Warn("OrderBook channel closed",
					zap.String("symbol", sub.Symbol))
				return
			}
			m.processOrderBook(orderBook) // 复用原有的处理逻辑
		}
	}
}

// processPrice 处理价格数据 (WatchPrice)
func (m *Miner) processPrice(watchPrice *ccxt.WatchPrice) {
	// 转换为标准格式的数据结构
	priceData := map[string]interface{}{
		"symbol":    watchPrice.Symbol,
		"timestamp": watchPrice.TimeStamp,
		"price":     watchPrice.Price,
		"last":      watchPrice.Price, // 使用价格作为最新价格
		"close":     watchPrice.Price, // 使用价格作为收盘价
		"channel":   watchPrice.StreamName,
	}

	// 创建并发布事件
	event := m.createEvent(EventPrice, priceData, watchPrice.StreamName)
	m.publishEvent(event)

	// 保存到存储 - 转换为protocol格式
	if tsStorage := m.storage.GetTimeSeriesStorage(); tsStorage != nil {
		protocolTicker := m.typeConverter.CCXTTickerToProtocol(m.exchange, &ccxt.Ticker{
			Symbol:    watchPrice.Symbol,
			TimeStamp: watchPrice.TimeStamp,
			Last:      watchPrice.Price, // 使用价格作为最新价格
			Close:     watchPrice.Price, // 使用价格作为收盘价
			// 其他字段使用默认值0
		})

		if err := tsStorage.SaveTickers(m.ctx, m.exchange, []*pb.Ticker{protocolTicker}); err != nil {
			m.incrementErrors()
			logger.Ctx(m.ctx).Error("Failed to save ticker", zap.Error(err))
			return
		}
	}

	m.incrementMessages()
	logger.Ctx(m.ctx).Debug("Processed price data",
		zap.String("symbol", watchPrice.Symbol),
		zap.Float64("price", watchPrice.Price))
}

// processOHLCV 处理K线数据
func (m *Miner) processOHLCV(watchOHLCV *ccxt.WatchOHLCV) {
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
		"channel":   watchOHLCV.StreamName,
	}

	// 创建并发布事件
	event := m.createEvent(EventKline, ohlcvData, watchOHLCV.StreamName)
	m.publishEvent(event)

	// 保存到存储 - 转换为protocol格式
	if tsStorage := m.storage.GetTimeSeriesStorage(); tsStorage != nil {
		// 将ccxt OHLCV转换为protocol Kline
		protocolKline := m.typeConverter.CCXTOHLCVToProtocolKline(m.exchange, watchOHLCV.Symbol, watchOHLCV.Timeframe, &ccxt.OHLCV{
			Timestamp: watchOHLCV.Timestamp,
			Open:      watchOHLCV.Open,
			High:      watchOHLCV.High,
			Low:       watchOHLCV.Low,
			Close:     watchOHLCV.Close,
			Volume:    watchOHLCV.Volume,
		})

		if err := tsStorage.SaveKlines(m.ctx, m.exchange, []*pb.Kline{protocolKline}); err != nil {
			m.incrementErrors()
			logger.Ctx(m.ctx).Error("Failed to save OHLCV", zap.Error(err))
			return
		}
	}

	m.incrementMessages()
	logger.Ctx(m.ctx).Debug("Processed OHLCV data",
		zap.String("symbol", watchOHLCV.Symbol),
		zap.String("interval", watchOHLCV.Timeframe))
}

// processTrade 处理交易数据
func (m *Miner) processTrade(watchTrade *ccxt.WatchTrade) {
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
		"channel":      watchTrade.StreamName,
	}

	event := m.createEvent(EventTrade, tradeData, watchTrade.StreamName)
	m.publishEvent(event)

	if tsStorage := m.storage.GetTimeSeriesStorage(); tsStorage != nil {
		protocolTrade := m.typeConverter.CCXTTradeToProtocol(m.exchange, &ccxt.Trade{
			ID:           watchTrade.ID,
			Symbol:       watchTrade.Symbol,
			Amount:       watchTrade.Amount,
			Price:        watchTrade.Price,
			Side:         watchTrade.Side,
			Type:         watchTrade.Type,
			Timestamp:    watchTrade.Timestamp,
			TakerOrMaker: watchTrade.TakerOrMaker,
			Cost:         watchTrade.Cost,
		})

		if err := tsStorage.SaveTrades(m.ctx, m.exchange, []*pb.Trade{protocolTrade}); err != nil {
			m.incrementErrors()
			logger.Ctx(m.ctx).Error("Failed to save trade", zap.Error(err))
			return
		}
	}

	m.incrementMessages()
	logger.Ctx(m.ctx).Debug("Processed trade data",
		zap.String("symbol", watchTrade.Symbol),
		zap.Float64("price", watchTrade.Price),
		zap.Float64("amount", watchTrade.Amount))
}

// processOrderBook 处理订单簿数据
func (m *Miner) processOrderBook(watchOrderBook *ccxt.WatchOrderBook) {
	// 转换为标准格式的数据结构
	orderBookData := map[string]interface{}{
		"symbol":    watchOrderBook.Symbol,
		"timestamp": watchOrderBook.TimeStamp,
		"bids":      watchOrderBook.Bids,
		"asks":      watchOrderBook.Asks,
		"nonce":     watchOrderBook.Nonce,
		"channel":   watchOrderBook.StreamName,
	}

	// 创建并发布事件
	event := m.createEvent(EventOrderBook, orderBookData, watchOrderBook.StreamName)
	m.publishEvent(event)

	// 可以选择保存到Redis缓存
	if kvStorage := m.storage.GetKVStorage(); kvStorage != nil {
		// 可以实现订单簿的Redis缓存逻辑
		logger.Ctx(m.ctx).Debug("OrderBook data processed (not saved to time series storage)",
			zap.String("symbol", watchOrderBook.Symbol))
	}

	m.incrementMessages()
	logger.Ctx(m.ctx).Debug("Processed orderbook data",
		zap.String("symbol", watchOrderBook.Symbol))
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

// publishEvent 统一的事件发布逻辑
func (m *Miner) publishEvent(event Event) {
	if m.publisher != nil {
		if err := m.publisher.Publish(m.ctx, event); err != nil {
			m.incrementErrors()
			logger.Ctx(m.ctx).Error("Failed to publish event",
				zap.String("type", string(event.Type)),
				zap.String("symbol", event.Symbol),
				zap.Error(err))
		}
	} else {
		logger.Ctx(m.ctx).Warn("Publisher not initialized",
			zap.String("type", string(event.Type)))
	}
}

// createEvent 创建标准化事件结构
func (m *Miner) createEvent(eventType EventType, data map[string]interface{}, streamName string) Event {
	event := Event{
		Type:      eventType,
		Source:    m.exchange,
		Market:    m.market,
		Symbol:    cast.ToString(data["symbol"]),
		Data:      data,
		Timestamp: time.Now().UnixMilli(),
	}

	// 统一的映射逻辑
	if sub, ok := m.subscriptions[streamName]; ok {
		event.Topic = sub.Topic
		event.Symbol = sub.Symbol

		data["symbol"] = sub.Symbol
		event.Data = data
	}

	return event
}
