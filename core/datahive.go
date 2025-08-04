package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/riven-blade/datahive/pkg/protocol/pb"
	"github.com/riven-blade/datahive/pkg/server"
	"github.com/riven-blade/datahive/storage"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/riven-blade/datahive/config"
	"github.com/riven-blade/datahive/pkg/ccxt"
	_ "github.com/riven-blade/datahive/pkg/ccxt/binance" // Register Binance exchange
	_ "github.com/riven-blade/datahive/pkg/ccxt/bybit"   // Register Bybit exchange
	"github.com/riven-blade/datahive/pkg/logger"
	"github.com/riven-blade/datahive/pkg/utils"

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

	// 并发控制
	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc

	// 存储服务
	storage *storage.StorageManager

	// 状态 - 使用原子操作 (0=stopped, 1=running)
	running atomic.Int32
}

// NewDataHive 创建数据中心
func NewDataHive(cfg *config.Config) *DataHive {
	if cfg == nil {
		cfg = config.DefaultConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	// 创建存储服务
	storageService, err := storage.NewStorageManager(ctx, cfg.QuestDB, cfg.Redis)
	if err != nil {
		logger.Ctx(context.Background()).Fatal("Failed to create storage service", zap.Error(err))
	}

	return &DataHive{
		config:  cfg,
		miners:  make(map[string]*Miner),
		ctx:     ctx,
		cancel:  cancel,
		storage: storageService,
	}
}

// Start 启动DataHive
func (d *DataHive) Start() error {
	// 使用原子操作检查并设置状态，完全避免锁
	if !d.running.CompareAndSwap(0, 1) {
		return fmt.Errorf("datahive already running")
	}

	// 在状态已设置后执行重量级操作
	logger.Ctx(d.ctx).Debug("🚀 开始初始化服务器（原子操作设置状态）")
	if err := d.initServer(); err != nil {
		// 如果失败，原子性地恢复状态
		d.running.Store(0)
		return fmt.Errorf("failed to init server: %w", err)
	}

	d.publisher = NewEventPublisher(d.server)

	// 现在所有关键组件都已初始化，可以安全启动服务器接收连接
	logger.Ctx(d.ctx).Debug("🔌 启动服务器监听连接")
	if err := d.server.Start(); err != nil {
		d.running.Store(0) // 恢复状态
		return fmt.Errorf("failed to start server: %w", err)
	}

	if err := d.startHealthCheck(); err != nil {
		logger.Ctx(d.ctx).Warn("Failed to start health check endpoint", zap.Error(err))
	}

	logger.Ctx(d.ctx).Info("✅ DataHive started successfully")
	return nil
}

// Stop 停止DataHive
func (d *DataHive) Stop() error {
	// 使用原子操作检查状态
	if !d.running.CompareAndSwap(1, 0) {
		return nil // 已经停止或未启动
	}

	// 获取锁来安全地访问miners集合
	d.mu.Lock()
	minersToStop := make(map[string]*Miner)
	for key, miner := range d.miners {
		minersToStop[key] = miner
	}
	d.mu.Unlock()

	// 在锁外停止所有矿工
	for key, miner := range minersToStop {
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

	logger.Ctx(d.ctx).Info("DataHive stopped")
	return nil
}

// Subscribe 订阅数据
func (d *DataHive) Subscribe(ctx context.Context, req SubscriptionRequest) ([]string, error) {
	miner, err := d.getOrCreateMiner(req.Exchange, req.Market)
	if err != nil {
		return nil, fmt.Errorf("failed to get miner: %w", err)
	}

	var topics []string
	var minerSubs []MinerSubscription

	var interval string
	var depth int

	switch req.Event {
	case EventKline:
		interval = req.Options.Interval
		if interval == "" {
			interval = "1m"
		}
	case EventOrderBook:
		depth = req.Options.Depth
	}

	// 生成
	topic := utils.GenerateTopic(req.Exchange, req.Market, req.Symbol, string(req.Event), interval, depth)
	minerSub := MinerSubscription{
		Symbol:   req.Symbol, // 用户请求的原始symbol
		Event:    req.Event,  // 单个事件类型
		Interval: interval,   // K线间隔
		Depth:    depth,      // 深度档位
		Topic:    topic,      // 预计算的topic作为订阅ID
	}

	minerSubs = append(minerSubs, minerSub)
	topics = append(topics, topic)

	// 提交所有订阅到Miner
	for i := range minerSubs {
		minerSub := minerSubs[i]
		if err := miner.Subscribe(&minerSub); err != nil {
			return nil, fmt.Errorf("failed to subscribe to miner for topic %s: %w", minerSub.Topic, err)
		}
	}

	logger.Ctx(ctx).Info("Started data collection",
		zap.String("exchange", req.Exchange),
		zap.String("market", req.Market),
		zap.String("symbol", req.Symbol),
		zap.String("event", string(req.Event)),
		zap.Strings("topics", topics))

	return topics, nil
}

// Health 获取服务健康状态
func (d *DataHive) Health() Health {
	// 读取原子状态，无需锁
	isRunning := d.running.Load() == 1
	status := "healthy"
	if !isRunning {
		status = "unhealthy"
	}

	// 快速读取统计信息
	d.mu.RLock()
	minersCount := len(d.miners)
	d.mu.RUnlock()

	details := map[string]string{
		"running": fmt.Sprintf("%t", isRunning),
		"miners":  fmt.Sprintf("%d", minersCount),
		"server":  "gnet",
	}

	return Health{
		Status:    status,
		Timestamp: time.Now().UnixMilli(),
		Details:   details,
	}
}

// =============================================================================
// 数据获取方法 - 用于handlers调用
// =============================================================================

// GetOrCreateMiner 获取或创建指定的Miner
func (d *DataHive) GetOrCreateMiner(exchange, market string) (*Miner, error) {
	if d.running.Load() == 0 {
		return nil, fmt.Errorf("datahive not running")
	}

	return d.getOrCreateMiner(exchange, market)
}

// FetchMarkets 通过指定交易所获取市场数据
func (d *DataHive) FetchMarkets(ctx context.Context, exchange, market string) ([]*ccxt.Market, error) {
	logger.Ctx(ctx).Debug("DataHive.FetchMarkets开始执行",
		zap.String("exchange", exchange),
		zap.String("market", market))

	miner, err := d.getOrCreateMiner(exchange, market)
	if err != nil {
		logger.Ctx(ctx).Error("获取或创建Miner失败", zap.Error(err))
		return nil, fmt.Errorf("failed to get miner: %w", err)
	}

	logger.Ctx(ctx).Debug("Miner获取成功，开始调用FetchMarkets")
	markets, err := miner.FetchMarkets(ctx)
	if err != nil {
		logger.Ctx(ctx).Error("Miner.FetchMarkets失败", zap.Error(err))
		return nil, err
	}

	logger.Ctx(ctx).Debug("FetchMarkets执行成功",
		zap.Int("markets_count", len(markets)))
	return markets, nil
}

// FetchTicker 通过指定交易所获取ticker数据
func (d *DataHive) FetchTicker(ctx context.Context, exchange, market, symbol string) (*ccxt.Ticker, error) {
	miner, err := d.GetOrCreateMiner(exchange, market)
	if err != nil {
		return nil, fmt.Errorf("failed to get miner: %w", err)
	}

	// 让交易所自己处理symbol格式
	return miner.FetchTicker(ctx, symbol)
}

// FetchOHLCV 通过指定交易所获取K线数据
func (d *DataHive) FetchOHLCV(ctx context.Context, exchange, market, symbol, timeframe string, since int64, limit int) ([]*ccxt.OHLCV, error) {
	miner, err := d.GetOrCreateMiner(exchange, market)
	if err != nil {
		return nil, fmt.Errorf("failed to get miner: %w", err)
	}

	// 让交易所自己处理symbol格式
	return miner.FetchOHLCV(ctx, symbol, timeframe, since, limit)
}

// FetchOrderBook 通过指定交易所获取订单簿数据
func (d *DataHive) FetchOrderBook(ctx context.Context, exchange, market, symbol string, limit int) (*ccxt.OrderBook, error) {
	miner, err := d.GetOrCreateMiner(exchange, market)
	if err != nil {
		return nil, fmt.Errorf("failed to get miner: %w", err)
	}

	// 让交易所自己处理symbol格式
	return miner.FetchOrderBook(ctx, symbol, limit)
}

// FetchTrades 通过指定交易所获取交易记录
func (d *DataHive) FetchTrades(ctx context.Context, exchange, market, symbol string, since, limit int) ([]*ccxt.Trade, error) {
	miner, err := d.GetOrCreateMiner(exchange, market)
	if err != nil {
		return nil, fmt.Errorf("failed to get miner: %w", err)
	}

	// 让交易所自己处理symbol格式
	return miner.FetchTrades(ctx, symbol, int64(since), limit)
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
	d.server.RegisterHandler(pb.ActionType_SUBSCRIBE, handlers.HandleSubscribe)
	d.server.RegisterHandler(pb.ActionType_UNSUBSCRIBE, handlers.HandleUnsubscribe)
	d.server.RegisterHandler(pb.ActionType_FETCH_MARKETS, handlers.HandleFetchMarkets)
	d.server.RegisterHandler(pb.ActionType_FETCH_TICKER, handlers.HandleFetchTicker)
	d.server.RegisterHandler(pb.ActionType_FETCH_TICKERS, handlers.HandleFetchTickers)
	d.server.RegisterHandler(pb.ActionType_FETCH_KLINES, handlers.HandleFetchKlines)
	d.server.RegisterHandler(pb.ActionType_FETCH_ORDERBOOK, handlers.HandleFetchOrderBook)
	d.server.RegisterHandler(pb.ActionType_FETCH_TRADES, handlers.HandleFetchTrades)

	// 5. 服务器已配置，但暂不启动 - 等待publisher初始化
	logger.Ctx(d.ctx).Info("Server configured successfully",
		zap.String("address", addr),
		zap.Int("handlers", 8))
	return nil
}

func (d *DataHive) getOrCreateMiner(exchange, market string) (*Miner, error) {
	// 检查DataHive是否已启动
	if d.running.Load() == 0 {
		return nil, fmt.Errorf("datahive not started yet")
	}

	ctx := d.ctx
	logger.Ctx(ctx).Debug("🔍 getOrCreateMiner开始执行",
		zap.String("exchange", exchange),
		zap.String("market", market))

	key := d.getMinerKey(exchange, market)
	logger.Ctx(ctx).Debug("🔑 生成Miner key", zap.String("key", key))

	logger.Ctx(ctx).Debug("🔒 准备获取读锁")
	d.mu.RLock()
	logger.Ctx(ctx).Debug("✅ 成功获取读锁")
	miner, exists := d.miners[key]
	d.mu.RUnlock()
	logger.Ctx(ctx).Debug("🔓 释放读锁", zap.Bool("exists", exists))

	if exists {
		logger.Ctx(ctx).Debug("✅ 找到已存在的Miner", zap.String("key", key))
		return miner, nil
	}

	logger.Ctx(ctx).Debug("🆕 需要创建新的Miner", zap.String("key", key))

	// 先在锁外执行重量级操作，避免阻塞其他请求
	logger.Ctx(ctx).Debug("⚙️ 开始创建交易所客户端")
	client, err := d.createExchangeClient(exchange, market)
	if err != nil {
		logger.Ctx(ctx).Error("创建交易所客户端失败", zap.Error(err))
		return nil, fmt.Errorf("failed to create exchange client: %w", err)
	}

	logger.Ctx(ctx).Debug("开始创建新的Miner")
	newMiner := NewMiner(client, exchange, market, d.publisher, d.storage)

	logger.Ctx(ctx).Debug("开始启动Miner")
	if err := newMiner.Start(d.ctx); err != nil {
		logger.Ctx(ctx).Error("启动Miner失败", zap.Error(err))
		return nil, fmt.Errorf("failed to start miner: %w", err)
	}

	// 只在最后才加写锁，快速完成注册
	d.mu.Lock()
	// 双重检查（在锁内再次检查，因为可能有并发创建）
	if existingMiner, exists := d.miners[key]; exists {
		d.mu.Unlock()
		// 已存在则停止新创建的miner，返回已存在的
		logger.Ctx(ctx).Debug("并发创建冲突，使用已存在的Miner", zap.String("key", key))
		newMiner.Stop(ctx) // 停止新创建的miner
		return existingMiner, nil
	}

	// 注册新的miner
	d.miners[key] = newMiner
	d.mu.Unlock()

	miner = newMiner

	logger.Ctx(ctx).Debug("Miner创建并启动成功",
		zap.String("exchange", exchange),
		zap.String("market", market),
		zap.String("key", key))

	return miner, nil
}

func (d *DataHive) createExchangeClient(exchange, market string) (ccxt.Exchange, error) {
	ctx := d.ctx
	logger.Ctx(ctx).Debug("开始创建交易所客户端",
		zap.String("exchange", exchange),
		zap.String("market", market))

	// Log supported exchanges for debugging
	supportedExchanges := ccxt.GetSupportedExchanges()
	logger.Ctx(ctx).Debug("支持的交易所列表",
		zap.Strings("exchanges", supportedExchanges))

	// 从配置中获取交易所配置
	cfg, err := d.createExchangeConfig(exchange)
	if err != nil {
		logger.Ctx(ctx).Error("创建交易所配置失败",
			zap.String("exchange", exchange),
			zap.Error(err))
		return nil, err
	}

	logger.Ctx(ctx).Debug("调用ccxt.CreateExchange",
		zap.String("exchange", exchange),
		zap.Bool("enableWebSocket", cfg.(*ccxt.BaseConfig).EnableWebSocket))

	client, err := ccxt.CreateExchange(exchange, cfg)
	if err != nil {
		logger.Ctx(ctx).Error("ccxt.CreateExchange失败",
			zap.String("exchange", exchange),
			zap.Error(err))
		return nil, err
	}

	logger.Ctx(ctx).Debug("ccxt.CreateExchange成功",
		zap.String("exchange", exchange))
	return client, nil
}

func (d *DataHive) getMinerKey(exchange, market string) string {
	return fmt.Sprintf("%s:%s", exchange, market)
}

// createExchangeConfig 根据配置创建交易所配置
func (d *DataHive) createExchangeConfig(exchange string) (ccxt.ExchangeConfig, error) {
	// 检查配置中是否有该交易所
	if d.config.Exchanges == nil {
		return nil, fmt.Errorf("no exchange configurations found")
	}

	exchangeConfig, exists := d.config.Exchanges[exchange]
	if !exists {
		return nil, fmt.Errorf("exchange %s not configured", exchange)
	}

	if !exchangeConfig.Enabled {
		return nil, fmt.Errorf("exchange %s is disabled", exchange)
	}

	// 创建BaseConfig
	cfg := ccxt.DefaultBaseConfig()

	// 从YAML配置设置值
	cfg.APIKey = exchangeConfig.APIKey
	cfg.Secret = exchangeConfig.Secret
	cfg.TestNet = exchangeConfig.TestNet
	cfg.RateLimit = exchangeConfig.RateLimit
	cfg.EnableWebSocket = exchangeConfig.EnableWebSocket // 这是关键！
	cfg.WSMaxReconnect = exchangeConfig.WSMaxReconnect

	if exchangeConfig.DefaultType != "" {
		cfg.DefaultType = exchangeConfig.DefaultType
	}

	if exchangeConfig.Timeout > 0 {
		cfg.Timeout = exchangeConfig.Timeout
	}

	return cfg, nil
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
			"miners_count":   len(d.miners),
			"uptime_seconds": time.Since(time.Unix(d.Health().Timestamp/1000, 0)).Seconds(),
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

func (d *DataHive) GetTopicSymbol(exchange, market, symbol string) (string, error) {
	miner, err := d.getOrCreateMiner(exchange, market)
	if err != nil {
		return "", fmt.Errorf("failed to get miner: %w", err)
	}

	marketsMap, err := miner.client.LoadMarkets(context.Background())
	if err != nil {
		return "", fmt.Errorf("failed to load markets: %w", err)
	}

	if len(marketsMap) == 0 {
		return "", fmt.Errorf("no markets available")
	}

	if marketInfo, exists := marketsMap[symbol]; exists {
		return marketInfo.ID, nil
	}

	return symbol, errors.New(fmt.Sprintf("markets: %s, no such symbol.", symbol))
}
