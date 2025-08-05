package storage

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/riven-blade/datahive/config"
	"github.com/riven-blade/datahive/pkg/logger"
	"github.com/riven-blade/datahive/pkg/protocol/pb"

	_ "github.com/lib/pq"
	"go.uber.org/zap"
)

// QuestDBStorage QuestDB 时序数据存储实现
type QuestDBStorage struct {
	config        *config.QuestDBConfig
	db            *sql.DB
	typeConverter *TypeConverter
	isOpen        int32
	mu            sync.RWMutex

	// 批量处理器
	batchProcessor *BatchProcessor

	// 健康检查和重连
	healthChecker   *HealthChecker
	lastHealthCheck time.Time
	healthStatus    int32

	// 统计信息
	stats *StorageStats

	// 关闭信号
	stopChan chan struct{}
	wg       sync.WaitGroup
}

// BatchProcessor 批量处理器
type BatchProcessor struct {
	storage       *QuestDBStorage
	flushInterval time.Duration
	maxBatchSize  int
	maxMemoryMB   int64 // 最大内存使用量(MB)

	// 各类型数据批次
	klineBatch      []*pb.Kline
	tradeBatch      []*pb.Trade
	tickerBatch     []*pb.Ticker
	miniTickerBatch []*pb.MiniTicker

	// 互斥锁
	klineMu      sync.Mutex
	tradeMu      sync.Mutex
	tickerMu     sync.Mutex
	miniTickerMu sync.Mutex

	// 内存使用统计
	memoryUsage int64 // 当前内存使用量(bytes)

	// 控制通道
	stopChan chan struct{}
	wg       sync.WaitGroup
}

// HealthChecker 健康检查器
type HealthChecker struct {
	storage       *QuestDBStorage
	checkInterval time.Duration
	timeout       time.Duration
	maxRetries    int
	retryDelay    time.Duration
	ticker        *time.Ticker
	stopChan      chan struct{}
	wg            sync.WaitGroup
}

// StorageStats 存储统计信息
type StorageStats struct {
	mu sync.RWMutex

	// 连接统计
	ConnectionCount    int64
	ReconnectionCount  int64
	LastConnectionTime time.Time

	// 操作统计
	TotalOperations  int64
	SuccessfulOps    int64
	FailedOps        int64
	LastError        time.Time
	LastErrorMessage string

	// 数据统计
	KlinesSaved  int64
	TradesSaved  int64
	TickersSaved int64

	// 性能统计
	AverageLatencyMs float64
	MaxLatencyMs     float64
	TotalProcessedMB float64
	QueriesPerSecond float64
}

// NewQuestDBStorage 创建并连接 QuestDB 存储实例
func NewQuestDBStorage(ctx context.Context, conf *config.QuestDBConfig) (*QuestDBStorage, error) {
	if conf == nil {
		conf = config.NewQuestDBConfig()
	}

	storage := &QuestDBStorage{
		config:        conf,
		typeConverter: NewTypeConverter(),
		stopChan:      make(chan struct{}),
		stats:         &StorageStats{},
	}

	// 初始化批量处理器
	storage.batchProcessor = &BatchProcessor{
		storage:       storage,
		flushInterval: conf.FlushInterval,
		maxBatchSize:  conf.BatchSize,
		maxMemoryMB:   100, // 默认100MB内存限制
		stopChan:      make(chan struct{}),
	}

	// 初始化健康检查器
	storage.healthChecker = &HealthChecker{
		storage:       storage,
		checkInterval: conf.HealthCheckInterval,
		timeout:       10 * time.Second,
		maxRetries:    3,
		retryDelay:    time.Second,
		stopChan:      make(chan struct{}),
	}

	// 立即建立连接
	if err := storage.Connect(ctx); err != nil {
		return nil, ErrConnectionError("failed to initialize QuestDB storage", err)
	}

	// 启动优雅退出监听
	go func() {
		<-ctx.Done()
		logger.Ctx(context.Background()).Info("收到退出信号，开始优雅关闭QuestDB存储...")
		if err := storage.Close(); err != nil {
			logger.Ctx(context.Background()).Error("QuestDB存储关闭失败", zap.Error(err))
		} else {
			logger.Ctx(context.Background()).Info("QuestDB存储已优雅关闭")
		}
	}()

	return storage, nil
}

// Connect 连接到 QuestDB
func (q *QuestDBStorage) Connect(ctx context.Context) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if atomic.LoadInt32(&q.isOpen) == 1 {
		return nil
	}

	q.recordConnectionAttempt()

	// 执行连接操作
	if err := q.connectWithRetry(ctx, 3); err != nil {
		return ErrConnectionError("failed to establish connection", err)
	}

	// 启动各种后台服务
	if err := q.startBackgroundServices(); err != nil {
		q.Close()
		return ErrConnectionError("failed to start background services", err)
	}

	atomic.StoreInt32(&q.isOpen, 1)
	atomic.StoreInt32(&q.healthStatus, 1)

	logger.Ctx(ctx).Info("QuestDB连接成功",
		zap.String("host", q.config.Host),
		zap.Int("port", q.config.Port),
		zap.String("database", q.config.Database))

	return nil
}

// Close 关闭连接
func (q *QuestDBStorage) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if atomic.LoadInt32(&q.isOpen) == 0 {
		return nil
	}

	// 发送停止信号
	close(q.stopChan)

	// 等待所有goroutine完成
	q.wg.Wait()

	// 停止各种服务
	q.batchProcessor.stop()
	q.healthChecker.stop()

	// 关闭数据库连接
	if q.db != nil {
		if err := q.db.Close(); err != nil {
			return ErrConnectionError("failed to close database connection", err)
		}
		q.db = nil
	}

	atomic.StoreInt32(&q.isOpen, 0)
	atomic.StoreInt32(&q.healthStatus, 0)

	logger.Ctx(context.Background()).Info("QuestDB连接已关闭")
	return nil
}

// Ping 检查连接状态
func (q *QuestDBStorage) Ping(ctx context.Context) error {
	if atomic.LoadInt32(&q.isOpen) == 0 {
		return ErrConnectionClosed
	}

	ctx, cancel := context.WithTimeout(ctx, q.config.QueryTimeout)
	defer cancel()

	if err := q.db.PingContext(ctx); err != nil {
		return ErrConnectionError("ping failed", err)
	}

	return nil
}

// IsHealthy 检查存储健康状态
func (q *QuestDBStorage) IsHealthy() bool {
	return atomic.LoadInt32(&q.isOpen) == 1 && atomic.LoadInt32(&q.healthStatus) == 1
}

// SaveKlines 保存K线数据
func (q *QuestDBStorage) SaveKlines(ctx context.Context, exchange string, klines []*pb.Kline) error {
	if !q.IsHealthy() {
		q.recordFailedOperation("storage not healthy")
		return ErrStorageNotHealthy
	}

	if len(klines) == 0 {
		return nil
	}

	start := time.Now()

	// 数据验证
	validKlines := make([]*pb.Kline, 0, len(klines))
	for _, kline := range klines {
		if err := q.typeConverter.ValidateKlineData(kline); err != nil {
			logger.Ctx(ctx).Warn("Invalid kline data",
				zap.String("exchange", exchange),
				zap.String("symbol", kline.Symbol),
				zap.Error(err))
			continue
		}
		validKlines = append(validKlines, kline)
	}

	if len(validKlines) == 0 {
		return ErrValidationError("no valid klines to save")
	}

	// 添加到批量处理器
	if err := q.batchProcessor.addKlines(validKlines); err != nil {
		q.recordFailedOperation("failed to add klines to batch")
		return ErrQueryError("failed to queue klines for saving", err)
	}

	// 记录统计信息
	q.recordSuccessfulOperation()
	q.stats.mu.Lock()
	q.stats.KlinesSaved += int64(len(validKlines))
	q.stats.AverageLatencyMs = q.updateAverageLatency(time.Since(start))
	q.stats.mu.Unlock()

	return nil
}

// QueryKlines 查询K线数据
func (q *QuestDBStorage) QueryKlines(ctx context.Context, exchange, symbol, timeframe string, start int64, limit int) ([]*pb.Kline, error) {
	if !q.IsHealthy() {
		q.recordFailedOperation("storage not healthy")
		return nil, ErrStorageNotHealthy
	}

	// 参数验证
	if exchange == "" || symbol == "" || timeframe == "" {
		return nil, ErrInvalidQuery
	}

	if limit <= 0 {
		limit = 1000 // 默认限制
	}
	if limit > 10000 {
		return nil, ErrInvalidData("limit too large, maximum is 10000")
	}

	queryStart := time.Now()
	startTime := time.UnixMilli(start)
	tableName := fmt.Sprintf("klines_%s", timeframe)

	query := fmt.Sprintf(`
		SELECT exchange, symbol, timeframe, open_time, open, high, low, close, volume, closed
		FROM %s 
		WHERE exchange = $1 AND symbol = $2 AND open_time >= $3 
		ORDER BY open_time ASC
		LIMIT $4`, tableName)

	ctx, cancel := context.WithTimeout(ctx, q.config.QueryTimeout)
	defer cancel()

	rows, err := q.db.QueryContext(ctx, query, exchange, symbol, startTime, limit)
	if err != nil {
		q.recordFailedOperation("query failed")
		return nil, ErrQueryError(fmt.Sprintf("failed to query klines from %s", tableName), err)
	}
	defer rows.Close()

	var klines []*pb.Kline
	for rows.Next() {
		kline := &pb.Kline{}
		var openTime time.Time

		err := rows.Scan(&kline.Exchange, &kline.Symbol, &kline.Timeframe,
			&openTime, &kline.Open, &kline.High, &kline.Low, &kline.Close, &kline.Volume, &kline.Closed)
		if err != nil {
			q.recordFailedOperation("scan failed")
			return nil, ErrQueryError("failed to scan kline data", err)
		}

		kline.OpenTime = openTime.UnixMilli()
		klines = append(klines, kline)
	}

	if err = rows.Err(); err != nil {
		q.recordFailedOperation("rows iteration failed")
		return nil, ErrQueryError("failed to iterate rows", err)
	}

	// 记录查询统计
	queryDuration := time.Since(queryStart)
	q.recordSuccessfulOperation()
	q.stats.mu.Lock()
	q.stats.AverageLatencyMs = q.updateAverageLatency(queryDuration)
	if queryDuration.Milliseconds() > int64(q.stats.MaxLatencyMs) {
		q.stats.MaxLatencyMs = float64(queryDuration.Milliseconds())
	}
	q.stats.mu.Unlock()

	return klines, nil
}

// SaveTrades 保存交易数据
func (q *QuestDBStorage) SaveTrades(ctx context.Context, exchange string, trades []*pb.Trade) error {
	if !q.IsHealthy() {
		q.recordFailedOperation("storage not healthy")
		return ErrStorageNotHealthy
	}

	if len(trades) == 0 {
		return nil
	}

	start := time.Now()

	// 数据验证
	validTrades := make([]*pb.Trade, 0, len(trades))
	for _, trade := range trades {
		if err := q.typeConverter.ValidateTradeData(trade); err != nil {
			logger.Ctx(ctx).Warn("Invalid trade data",
				zap.String("exchange", exchange),
				zap.String("symbol", trade.Symbol),
				zap.String("tradeId", trade.Id),
				zap.Error(err))
			continue
		}
		validTrades = append(validTrades, trade)
	}

	if len(validTrades) == 0 {
		return ErrValidationError("no valid trades to save")
	}

	// 添加到批量处理器
	if err := q.batchProcessor.addTrades(validTrades); err != nil {
		q.recordFailedOperation("failed to add trades to batch")
		return ErrQueryError("failed to queue trades for saving", err)
	}

	// 记录统计信息
	q.recordSuccessfulOperation()
	q.stats.mu.Lock()
	q.stats.TradesSaved += int64(len(validTrades))
	q.stats.AverageLatencyMs = q.updateAverageLatency(time.Since(start))
	q.stats.mu.Unlock()

	return nil
}

// QueryTrades 查询交易数据
func (q *QuestDBStorage) QueryTrades(ctx context.Context, exchange, symbol string, start int64, limit int) ([]*pb.Trade, error) {
	if !q.IsHealthy() {
		q.recordFailedOperation("storage not healthy")
		return nil, ErrStorageNotHealthy
	}

	// 参数验证
	if exchange == "" || symbol == "" {
		return nil, ErrInvalidQuery
	}

	if limit <= 0 {
		limit = 1000
	}
	if limit > 10000 {
		return nil, ErrInvalidData("limit too large, maximum is 10000")
	}

	queryStart := time.Now()
	startTime := time.UnixMilli(start)

	query := `
		SELECT exchange, symbol, id, price, quantity, side, timestamp
		FROM trades 
		WHERE exchange = $1 AND symbol = $2 AND timestamp >= $3 
		ORDER BY timestamp ASC
		LIMIT $4`

	ctx, cancel := context.WithTimeout(ctx, q.config.QueryTimeout)
	defer cancel()

	rows, err := q.db.QueryContext(ctx, query, exchange, symbol, startTime, limit)
	if err != nil {
		q.recordFailedOperation("query failed")
		return nil, ErrQueryError("failed to query trades", err)
	}
	defer rows.Close()

	var trades []*pb.Trade
	for rows.Next() {
		trade := &pb.Trade{}
		var timestamp time.Time

		err := rows.Scan(&trade.Exchange, &trade.Symbol, &trade.Id,
			&trade.Price, &trade.Quantity, &trade.Side, &timestamp)
		if err != nil {
			q.recordFailedOperation("scan failed")
			return nil, ErrQueryError("failed to scan trade data", err)
		}

		trade.Timestamp = timestamp.UnixMilli()
		trades = append(trades, trade)
	}

	if err = rows.Err(); err != nil {
		q.recordFailedOperation("rows iteration failed")
		return nil, ErrQueryError("failed to iterate rows", err)
	}

	// 记录查询统计
	queryDuration := time.Since(queryStart)
	q.recordSuccessfulOperation()
	q.stats.mu.Lock()
	q.stats.AverageLatencyMs = q.updateAverageLatency(queryDuration)
	q.stats.mu.Unlock()

	return trades, nil
}

// SaveTickers 保存价格数据
func (q *QuestDBStorage) SaveTickers(ctx context.Context, exchange string, tickers []*pb.Ticker) error {
	if !q.IsHealthy() {
		q.recordFailedOperation("storage not healthy")
		return ErrStorageNotHealthy
	}

	if len(tickers) == 0 {
		return nil
	}

	start := time.Now()

	// 数据验证并设置exchange字段
	validTickers := make([]*pb.Ticker, 0, len(tickers))
	for _, ticker := range tickers {
		if err := q.typeConverter.ValidateTickerData(ticker); err != nil {
			logger.Ctx(ctx).Warn("Invalid ticker data",
				zap.String("exchange", exchange),
				zap.String("symbol", ticker.Symbol),
				zap.Error(err))
			continue
		}
		validTickers = append(validTickers, ticker)
	}

	if len(validTickers) == 0 {
		return ErrValidationError("no valid tickers to save")
	}

	// 添加到批量处理器，传递exchange信息
	if err := q.batchProcessor.addTickersWithExchange(exchange, validTickers); err != nil {
		q.recordFailedOperation("failed to add tickers to batch")
		return ErrQueryError("failed to queue tickers for saving", err)
	}

	// 记录统计信息
	q.recordSuccessfulOperation()
	q.stats.mu.Lock()
	q.stats.TickersSaved += int64(len(validTickers))
	q.stats.AverageLatencyMs = q.updateAverageLatency(time.Since(start))
	q.stats.mu.Unlock()

	return nil
}

// QueryTickers 查询价格数据
func (q *QuestDBStorage) QueryTickers(ctx context.Context, exchange, symbol string, start int64, limit int) ([]*pb.Ticker, error) {
	if !q.IsHealthy() {
		q.recordFailedOperation("storage not healthy")
		return nil, ErrStorageNotHealthy
	}

	// 参数验证
	if exchange == "" || symbol == "" {
		return nil, ErrInvalidQuery
	}

	if limit <= 0 {
		limit = 1000
	}
	if limit > 10000 {
		return nil, ErrInvalidData("limit too large, maximum is 10000")
	}

	queryStart := time.Now()
	startTime := time.UnixMilli(start)

	query := `
		SELECT symbol, timestamp, last, bid, ask, high, low, open, close, volume, change, change_percent
		FROM tickers 
		WHERE exchange = $1 AND symbol = $2 AND timestamp >= $3 
		ORDER BY timestamp ASC
		LIMIT $4`

	ctx, cancel := context.WithTimeout(ctx, q.config.QueryTimeout)
	defer cancel()

	rows, err := q.db.QueryContext(ctx, query, exchange, symbol, startTime, limit)
	if err != nil {
		q.recordFailedOperation("query failed")
		return nil, ErrQueryError("failed to query tickers", err)
	}
	defer rows.Close()

	var tickers []*pb.Ticker
	for rows.Next() {
		ticker := &pb.Ticker{}
		var timestamp time.Time

		err := rows.Scan(&ticker.Symbol, &timestamp, &ticker.Last, &ticker.Bid,
			&ticker.Ask, &ticker.High, &ticker.Low, &ticker.Open, &ticker.Close,
			&ticker.Volume, &ticker.Change, &ticker.ChangePercent)
		if err != nil {
			q.recordFailedOperation("scan failed")
			return nil, ErrQueryError("failed to scan ticker data", err)
		}

		ticker.Timestamp = timestamp.UnixMilli()
		tickers = append(tickers, ticker)
	}

	if err = rows.Err(); err != nil {
		q.recordFailedOperation("rows iteration failed")
		return nil, ErrQueryError("failed to iterate rows", err)
	}

	// 记录查询统计
	queryDuration := time.Since(queryStart)
	q.recordSuccessfulOperation()
	q.stats.mu.Lock()
	q.stats.AverageLatencyMs = q.updateAverageLatency(queryDuration)
	q.stats.mu.Unlock()

	return tickers, nil
}

// DeleteExpiredData 删除过期数据
func (q *QuestDBStorage) DeleteExpiredData(ctx context.Context, beforeTime int64) error {
	if !q.IsHealthy() {
		q.recordFailedOperation("storage not healthy")
		return ErrStorageNotHealthy
	}

	beforeTimeStamp := time.UnixMilli(beforeTime)

	ctx, cancel := context.WithTimeout(ctx, q.config.QueryTimeout*2) // 删除操作可能需要更长时间
	defer cancel()

	start := time.Now()

	// 删除过期的交易和价格数据
	tables := []string{"trades", "tickers"}
	for _, table := range tables {
		result, err := q.db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE timestamp < $1", table), beforeTimeStamp)
		if err != nil {
			q.recordFailedOperation("delete failed")
			return ErrQueryError(fmt.Sprintf("failed to delete old data from %s", table), err)
		}

		// 记录删除的行数
		if rowsAffected, err := result.RowsAffected(); err == nil {
			logger.Ctx(ctx).Info("Deleted expired data",
				zap.String("table", table),
				zap.Int64("rows", rowsAffected),
				zap.Int64("beforeTime", beforeTime))
		}
	}

	// 删除过期的K线数据（多个时间周期表）
	timeframes := []string{"1m", "5m", "15m", "1h", "4h", "1d"}
	for _, timeframe := range timeframes {
		tableName := fmt.Sprintf("klines_%s", timeframe)
		result, err := q.db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE open_time < $1", tableName), beforeTimeStamp)
		if err != nil {
			q.recordFailedOperation("delete failed")
			return ErrQueryError(fmt.Sprintf("failed to delete old data from %s", tableName), err)
		}

		if rowsAffected, err := result.RowsAffected(); err == nil {
			logger.Ctx(ctx).Info("Deleted expired klines",
				zap.String("table", tableName),
				zap.Int64("rows", rowsAffected),
				zap.Int64("beforeTime", beforeTime))
		}
	}

	q.recordSuccessfulOperation()
	logger.Ctx(ctx).Info("Expired data cleanup completed",
		zap.Int64("beforeTime", beforeTime),
		zap.Duration("duration", time.Since(start)))

	return nil
}

// SaveMiniTickers 保存MiniTicker数据
func (q *QuestDBStorage) SaveMiniTickers(ctx context.Context, exchange string, miniTickers []*pb.MiniTicker) error {
	if !q.IsHealthy() {
		q.recordFailedOperation("storage not healthy")
		return ErrStorageNotHealthy
	}

	if len(miniTickers) == 0 {
		return nil
	}

	start := time.Now()

	// 数据验证
	validMiniTickers := make([]*pb.MiniTicker, 0, len(miniTickers))
	for _, miniTicker := range miniTickers {
		if err := q.typeConverter.ValidateMiniTickerData(miniTicker); err != nil {
			logger.Ctx(ctx).Warn("Invalid mini ticker data",
				zap.String("exchange", exchange),
				zap.String("symbol", miniTicker.Symbol),
				zap.Error(err))
			continue
		}
		validMiniTickers = append(validMiniTickers, miniTicker)
	}

	if len(validMiniTickers) == 0 {
		return ErrValidationError("no valid mini tickers to save")
	}

	// 添加到批量处理器
	if err := q.batchProcessor.addMiniTickersWithExchange(exchange, validMiniTickers); err != nil {
		q.recordFailedOperation("failed to add mini tickers to batch")
		return ErrQueryError("failed to queue mini tickers for saving", err)
	}

	// 记录统计信息
	q.recordSuccessfulOperation()
	q.stats.mu.Lock()
	q.stats.TickersSaved += int64(len(validMiniTickers)) // 使用TickersSaved统计
	q.stats.AverageLatencyMs = q.updateAverageLatency(time.Since(start))
	q.stats.mu.Unlock()

	return nil
}

// QueryMiniTickers 查询MiniTicker数据
func (q *QuestDBStorage) QueryMiniTickers(ctx context.Context, exchange, symbol string, start int64, limit int) ([]*pb.MiniTicker, error) {
	if !q.IsHealthy() {
		q.recordFailedOperation("storage not healthy")
		return nil, ErrStorageNotHealthy
	}

	if exchange == "" || symbol == "" {
		return nil, ErrInvalidQuery
	}

	if limit <= 0 {
		limit = 1000
	}
	if limit > 10000 {
		return nil, ErrInvalidData("limit too large, maximum is 10000")
	}

	queryStart := time.Now()
	startTime := time.UnixMilli(start)

	query := `
		SELECT symbol, timestamp, open, high, low, close, volume, quote_volume
		FROM mini_tickers 
		WHERE exchange = $1 AND symbol = $2 AND timestamp >= $3 
		ORDER BY timestamp ASC
		LIMIT $4`

	ctx, cancel := context.WithTimeout(ctx, q.config.QueryTimeout)
	defer cancel()

	rows, err := q.db.QueryContext(ctx, query, exchange, symbol, startTime, limit)
	if err != nil {
		q.recordFailedOperation("query failed")
		return nil, ErrQueryError("failed to query mini tickers", err)
	}
	defer rows.Close()

	var miniTickers []*pb.MiniTicker
	for rows.Next() {
		miniTicker := &pb.MiniTicker{}
		if err := rows.Scan(
			&miniTicker.Symbol,
			&miniTicker.Timestamp,
			&miniTicker.Open,
			&miniTicker.High,
			&miniTicker.Low,
			&miniTicker.Close,
			&miniTicker.Volume,
			&miniTicker.QuoteVolume,
		); err != nil {
			logger.Ctx(ctx).Error("Failed to scan mini ticker row", zap.Error(err))
			continue
		}
		miniTickers = append(miniTickers, miniTicker)
	}

	if err := rows.Err(); err != nil {
		q.recordFailedOperation("row iteration failed")
		return nil, ErrQueryError("error iterating mini ticker rows", err)
	}

	// 记录统计信息
	queryDuration := time.Since(queryStart)
	q.recordSuccessfulOperation()

	q.stats.mu.Lock()
	q.stats.AverageLatencyMs = q.updateAverageLatency(queryDuration)
	if queryDuration.Milliseconds() > int64(q.stats.MaxLatencyMs) {
		q.stats.MaxLatencyMs = float64(queryDuration.Milliseconds())
	}
	q.stats.mu.Unlock()

	return miniTickers, nil
}
