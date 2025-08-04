package core

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"datahive/config"
	"datahive/pkg/ccxt"
	"datahive/pkg/logger"
	"datahive/pkg/protocol"
	"datahive/server"

	"go.uber.org/zap"
)

// DataHive 现代化数据中心
type DataHive struct {
	// 核心组件
	config    *config.Config
	server    server.Transport
	publisher Publisher

	// 数据采集器管理
	miners map[string]*Miner

	// 订阅管理
	subscriptions map[string]*Subscription

	// 并发控制
	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc

	// 状态
	running bool
}

// NewDataHive 创建数据中心
func NewDataHive(cfg *config.Config) *DataHive {
	if cfg == nil {
		cfg = config.DefaultConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &DataHive{
		config:        cfg,
		miners:        make(map[string]*Miner),
		subscriptions: make(map[string]*Subscription),
		ctx:           ctx,
		cancel:        cancel,
	}
}

// Start 启动DataHive
func (d *DataHive) Start() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.running {
		return fmt.Errorf("datahive already running")
	}

	if err := d.initServer(); err != nil {
		return fmt.Errorf("failed to init server: %w", err)
	}

	d.publisher = NewEventPublisher(d.server)

	d.storage = NewUnifiedStorage(nil)

	if err := d.startHealthCheck(); err != nil {
		logger.Ctx(d.ctx).Warn("Failed to start health check endpoint", zap.Error(err))
	}

	d.running = true
	logger.Ctx(d.ctx).Info("✅ DataHive started successfully")

	return nil
}

// Stop 停止DataHive
func (d *DataHive) Stop() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if !d.running {
		return nil
	}

	// 停止所有矿工
	for key, miner := range d.miners {
		if err := miner.Stop(d.ctx); err != nil {
			logger.Ctx(d.ctx).Error("Failed to stop miner", zap.String("key", key), zap.Error(err))
		}
	}

	// 停止服务器
	if d.server != nil {
		if err := d.server.Stop(); err != nil {
			logger.Ctx(d.ctx).Error("Failed to stop server", zap.Error(err))
		}
	}

	d.cancel()
	d.running = false

	logger.Ctx(d.ctx).Info("DataHive stopped")
	return nil
}

// Subscribe 订阅数据
func (d *DataHive) Subscribe(ctx context.Context, req SubscriptionRequest) (*Subscription, error) {
	miner, err := d.getOrCreateMiner(req.Exchange, req.Market)
	if err != nil {
		return nil, fmt.Errorf("failed to get miner: %w", err)
	}

	sub := &Subscription{
		ID:        generateID(),
		Request:   req,
		Status:    StatusActive,
		CreatedAt: time.Now().UnixMilli(),
	}

	for _, symbol := range req.Symbols {
		minerSub := MinerSubscription{
			Symbol:  symbol,
			Events:  req.Events,
			Options: req.Options,
		}

		if err := miner.Subscribe(minerSub); err != nil {
			return nil, fmt.Errorf("failed to subscribe to miner: %w", err)
		}
	}

	d.mu.Lock()
	d.subscriptions[sub.ID] = sub
	d.mu.Unlock()

	logger.Ctx(ctx).Info("Created subscription",
		zap.String("id", sub.ID),
		zap.String("exchange", req.Exchange),
		zap.String("market", req.Market),
		zap.Strings("symbols", req.Symbols))
	return sub, nil
}

// Unsubscribe 取消订阅
func (d *DataHive) Unsubscribe(ctx context.Context, id string) error {
	d.mu.Lock()
	sub, exists := d.subscriptions[id]
	if exists {
		delete(d.subscriptions, id)
	}
	d.mu.Unlock()

	if !exists {
		return fmt.Errorf("subscription not found: %s", id)
	}

	key := d.getMinerKey(sub.Request.Exchange, sub.Request.Market)
	if miner, exists := d.miners[key]; exists {
		if err := miner.Unsubscribe(id); err != nil {
			logger.Ctx(ctx).Error("Failed to unsubscribe from miner", zap.Error(err))
		}
	}

	logger.Ctx(ctx).Info("Removed subscription", zap.String("id", id))
	return nil
}

// UnsubscribeByRequest 根据订阅请求取消订阅
func (d *DataHive) UnsubscribeByRequest(ctx context.Context, req SubscriptionRequest) error {
	// 获取矿工
	miner, err := d.getOrCreateMiner(req.Exchange, req.Market)
	if err != nil {
		return fmt.Errorf("failed to get miner: %w", err)
	}

	// 从矿工取消订阅
	for _, symbol := range req.Symbols {
		for _, eventType := range req.Events {
			if err := miner.UnsubscribeBySymbolAndEvent(symbol, eventType); err != nil {
				logger.Ctx(ctx).Error("Failed to unsubscribe from miner",
					zap.String("symbol", symbol),
					zap.String("event", string(eventType)),
					zap.Error(err))
			}
		}
	}

	logger.Ctx(ctx).Info("Unsubscribed by request",
		zap.String("exchange", req.Exchange),
		zap.String("market", req.Market),
		zap.Strings("symbols", req.Symbols),
		zap.Int("events", len(req.Events)))

	return nil
}

// Health 获取健康状态
func (d *DataHive) Health() Health {
	d.mu.RLock()
	defer d.mu.RUnlock()

	status := "healthy"
	if !d.running {
		status = "unhealthy"
	}

	details := map[string]string{
		"running":       fmt.Sprintf("%t", d.running),
		"miners":        fmt.Sprintf("%d", len(d.miners)),
		"subscriptions": fmt.Sprintf("%d", len(d.subscriptions)),
	}

	return Health{
		Status:    status,
		Details:   details,
		Timestamp: time.Now().UnixMilli(),
	}
}

// =============================================================================
// 数据获取方法 - 用于handlers调用
// =============================================================================

// GetOrCreateMiner 获取或创建指定交易所的Miner
func (d *DataHive) GetOrCreateMiner(exchange, market string) (*Miner, error) {
	d.mu.RLock()
	if !d.running {
		d.mu.RUnlock()
		return nil, fmt.Errorf("datahive not running")
	}
	d.mu.RUnlock()

	return d.getOrCreateMiner(exchange, market)
}

// FetchMarkets 通过指定交易所获取市场数据
func (d *DataHive) FetchMarkets(ctx context.Context, exchange, market string) ([]*ccxt.Market, error) {
	miner, err := d.GetOrCreateMiner(exchange, market)
	if err != nil {
		return nil, fmt.Errorf("failed to get miner: %w", err)
	}

	return miner.FetchMarkets(ctx)
}

// FetchTicker 通过指定交易所获取ticker数据
func (d *DataHive) FetchTicker(ctx context.Context, exchange, market, symbol string) (*ccxt.Ticker, error) {
	miner, err := d.GetOrCreateMiner(exchange, market)
	if err != nil {
		return nil, fmt.Errorf("failed to get miner: %w", err)
	}

	return miner.FetchTicker(ctx, symbol)
}

// FetchOHLCV 通过指定交易所获取K线数据
func (d *DataHive) FetchOHLCV(ctx context.Context, exchange, market, symbol, timeframe string, since int64, limit int) ([]*ccxt.OHLCV, error) {
	miner, err := d.GetOrCreateMiner(exchange, market)
	if err != nil {
		return nil, fmt.Errorf("failed to get miner: %w", err)
	}

	return miner.FetchOHLCV(ctx, symbol, timeframe, since, limit)
}

// FetchOrderBook 通过指定交易所获取订单簿数据
func (d *DataHive) FetchOrderBook(ctx context.Context, exchange, market, symbol string, limit int) (*ccxt.OrderBook, error) {
	miner, err := d.GetOrCreateMiner(exchange, market)
	if err != nil {
		return nil, fmt.Errorf("failed to get miner: %w", err)
	}

	return miner.FetchOrderBook(ctx, symbol, limit)
}

// =============================================================================
// 私有方法
// =============================================================================

func (d *DataHive) initServer() error {
	// 1. 创建服务器配置
	addr := fmt.Sprintf("%s:%d", d.config.Server.Host, d.config.Server.Port)

	// 2. 创建server transport (使用默认配置)
	transport, err := server.CreateGNetServer(addr, nil)
	if err != nil {
		return fmt.Errorf("failed to create server transport: %w", err)
	}

	d.server = transport

	// 4. 创建并注册handlers
	handlers := NewHandlers(d)

	// 注册核心业务处理器
	d.server.RegisterHandler(protocol.ActionSubscribe, handlers.HandleSubscribe)
	d.server.RegisterHandler(protocol.ActionUnsubscribe, handlers.HandleUnsubscribe)
	d.server.RegisterHandler(protocol.ActionFetchMarkets, handlers.HandleFetchMarkets)
	d.server.RegisterHandler(protocol.ActionFetchTicker, handlers.HandleFetchTicker)
	d.server.RegisterHandler(protocol.ActionFetchKlines, handlers.HandleFetchKlines)
	d.server.RegisterHandler(protocol.ActionFetchOrderBook, handlers.HandleFetchOrderBook)

	// 5. 启动server
	if err := d.server.Start(); err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}

	logger.Ctx(d.ctx).Info("Server initialized successfully",
		zap.String("address", addr),
		zap.Int("handlers", 6))
	return nil
}

func (d *DataHive) getOrCreateMiner(exchange, market string) (*Miner, error) {
	key := d.getMinerKey(exchange, market)

	d.mu.RLock()
	miner, exists := d.miners[key]
	d.mu.RUnlock()

	if exists {
		return miner, nil
	}

	// 创建新矿工
	d.mu.Lock()
	defer d.mu.Unlock()

	// 双重检查
	if miner, exists := d.miners[key]; exists {
		return miner, nil
	}

	// 创建交易所客户端
	client, err := d.createExchangeClient(exchange, market)
	if err != nil {
		return nil, fmt.Errorf("failed to create exchange client: %w", err)
	}

	// 创建新的矿工
	miner = NewMiner(client, exchange, market, d.publisher, d.storage)

	// 启动矿工
	if err := miner.Start(d.ctx); err != nil {
		return nil, fmt.Errorf("failed to start miner: %w", err)
	}

	d.miners[key] = miner

	logger.Ctx(d.ctx).Info("Created new miner",
		zap.String("exchange", exchange),
		zap.String("market", market))

	return miner, nil
}

func (d *DataHive) createExchangeClient(exchange, market string) (ccxt.Exchange, error) {
	// 简化实现
	config := ccxt.DefaultConfig()
	return ccxt.CreateExchange(exchange, config)
}

func (d *DataHive) getMinerKey(exchange, market string) string {
	return fmt.Sprintf("%s:%s", exchange, market)
}

// generateID
func generateID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// startHealthCheck 启动HTTP健康检查端点
func (d *DataHive) startHealthCheck() error {
	// 健康检查端口比主服务端口+1000
	healthPort := d.config.Server.Port + 1000
	healthAddr := fmt.Sprintf(":%d", healthPort)

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		health := d.Health()
		w.Header().Set("Content-Type", "application/json")

		statusCode := http.StatusOK
		if health.Status != "healthy" {
			statusCode = http.StatusServiceUnavailable
		}

		w.WriteHeader(statusCode)
		json.NewEncoder(w).Encode(health)
	})

	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		d.mu.RLock()
		defer d.mu.RUnlock()

		metrics := map[string]interface{}{
			"miners_count":        len(d.miners),
			"subscriptions_count": len(d.subscriptions),
			"uptime_seconds":      time.Since(time.Unix(d.Health().Timestamp/1000, 0)).Seconds(),
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(metrics)
	})

	go func() {
		logger.Ctx(d.ctx).Info("Starting health check server", zap.String("addr", healthAddr))
		if err := http.ListenAndServe(healthAddr, nil); err != nil {
			logger.Ctx(d.ctx).Error("Health check server failed", zap.Error(err))
		}
	}()

	return nil
}
