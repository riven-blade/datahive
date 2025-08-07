package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/riven-blade/datahive/pkg/logger"

	"go.uber.org/zap"
)

// 内部连接和管理方法

// connectWithRetry 带重试的连接
func (q *QuestDBStorage) connectWithRetry(ctx context.Context, maxRetries int) error {
	var lastErr error

	for i := 0; i < maxRetries; i++ {
		connectCtx, cancel := context.WithTimeout(ctx, q.config.ConnectionTimeout)
		err := q.connectDatabase(connectCtx)
		cancel()

		if err == nil {
			q.recordSuccessfulOperation()
			return nil
		}

		lastErr = err
		q.recordFailedOperation("connection failed")

		if i < maxRetries-1 {
			logger.Ctx(ctx).Warn("连接失败，准备重试",
				zap.Error(err),
				zap.Int("attempt", i+1),
				zap.Int("maxRetries", maxRetries))
			time.Sleep(time.Duration(i+1) * time.Second)
		}
	}

	return ErrConnectionError(fmt.Sprintf("连接失败，已尝试 %d 次", maxRetries), lastErr)
}

// connectDatabase 实际的数据库连接逻辑
func (q *QuestDBStorage) connectDatabase(ctx context.Context) error {
	var dsn string
	if q.config.Username != "" && q.config.Password != "" {
		dsn = fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable&connect_timeout=%d",
			q.config.Username, q.config.Password, q.config.Host, q.config.Port,
			q.config.Database, int(q.config.ConnectionTimeout.Seconds()))
	} else {
		dsn = fmt.Sprintf("postgres://%s:%d/%s?sslmode=disable&connect_timeout=%d",
			q.config.Host, q.config.Port, q.config.Database,
			int(q.config.ConnectionTimeout.Seconds()))
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return ErrConnectionError("failed to open connection", err)
	}

	// 优化连接池配置
	db.SetMaxOpenConns(q.config.MaxConnections)
	db.SetMaxIdleConns(q.config.MaxConnections / 2)
	db.SetConnMaxLifetime(time.Hour)
	db.SetConnMaxIdleTime(30 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return ErrConnectionError("failed to ping database", err)
	}

	q.db = db

	// 初始化数据库表结构
	if err := q.initTables(ctx); err != nil {
		db.Close()
		return ErrConnectionError("failed to init tables", err)
	}

	return nil
}

// startBackgroundServices 启动后台服务
func (q *QuestDBStorage) startBackgroundServices() error {
	// 启动批量处理器
	if err := q.batchProcessor.start(); err != nil {
		return ErrConnectionError("failed to start batch processor", err)
	}

	// 启动健康检查器
	if q.config.EnableAutoReconnect {
		if err := q.healthChecker.start(); err != nil {
			return ErrConnectionError("failed to start health checker", err)
		}
	}

	return nil
}

// recreateTablesIfNeeded 检查并重建有问题的表
func (q *QuestDBStorage) recreateTablesIfNeeded(ctx context.Context) error {
	logger.Ctx(ctx).Info("检查表结构...")

	// 检查trades表的列结构
	rows, err := q.db.QueryContext(ctx, "SHOW COLUMNS FROM trades")
	if err != nil {
		// 表不存在，无需重建
		logger.Ctx(ctx).Debug("trades表不存在，将创建新表")
		return nil
	}
	defer rows.Close()

	// 检查是否有quantity列
	hasQuantityColumn := false
	for rows.Next() {
		var columnName, dataType string
		var indexed, designated bool
		if err := rows.Scan(&columnName, &dataType, &indexed, &designated); err != nil {
			continue
		}
		if columnName == "quantity" {
			hasQuantityColumn = true
			break
		}
	}

	// 如果没有quantity列，重建表
	if !hasQuantityColumn {
		logger.Ctx(ctx).Info("trades表结构不正确，重建表...")

		// 备份旧数据（如果有的话）
		backupQuery := `CREATE TABLE trades_backup AS (SELECT * FROM trades)`
		if _, err := q.db.ExecContext(ctx, backupQuery); err != nil {
			logger.Ctx(ctx).Debug("无法备份trades表数据", zap.Error(err))
		}

		// 删除旧表
		if _, err := q.db.ExecContext(ctx, "DROP TABLE IF EXISTS trades"); err != nil {
			return fmt.Errorf("failed to drop trades table: %w", err)
		}

		logger.Ctx(ctx).Info("trades表已重建")
	}

	return nil
}

// initTables 初始化表结构
func (q *QuestDBStorage) initTables(ctx context.Context) error {
	logger.Ctx(ctx).Info("初始化数据库表结构...")

	// 检查并重建有问题的表
	if err := q.recreateTablesIfNeeded(ctx); err != nil {
		logger.Ctx(ctx).Warn("重建表失败，继续使用现有表", zap.Error(err))
	}

	// 创建多个时间周期的K线数据表
	timeframes := []string{"1m", "5m", "15m", "1h", "4h", "1d"}
	for _, timeframe := range timeframes {
		tableName := fmt.Sprintf("klines_%s", timeframe)
		createKlinesTable := fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				exchange    SYMBOL,
				symbol      SYMBOL,
				timeframe   SYMBOL,
				open_time   TIMESTAMP,
				open        DOUBLE,
				high        DOUBLE,
				low         DOUBLE,
				close       DOUBLE,
				volume      DOUBLE,
				closed      BOOLEAN
			) TIMESTAMP(open_time) PARTITION BY DAY
			DEDUP UPSERT KEYS(open_time, exchange, symbol);
		`, tableName)

		if _, err := q.db.ExecContext(ctx, createKlinesTable); err != nil {
			return ErrQueryError(fmt.Sprintf("failed to create %s table", tableName), err)
		}
		logger.Ctx(ctx).Debug("创建K线表", zap.String("table", tableName))
	}

	// 创建交易数据表
	createTradesTable := `
		CREATE TABLE IF NOT EXISTS trades (
			exchange    SYMBOL,
			symbol      SYMBOL,
			id          STRING,
			price       DOUBLE,
			quantity    DOUBLE,
			side        SYMBOL,
			timestamp   TIMESTAMP
		) TIMESTAMP(timestamp) PARTITION BY DAY
		DEDUP UPSERT KEYS(timestamp, exchange, symbol, id);
	`

	if _, err := q.db.ExecContext(ctx, createTradesTable); err != nil {
		return ErrQueryError("failed to create trades table", err)
	}

	// 创建价格数据表
	createTickersTable := `
		CREATE TABLE IF NOT EXISTS tickers (
			exchange       SYMBOL,
			symbol         SYMBOL,
			timestamp      TIMESTAMP,
			last           DOUBLE,
			bid            DOUBLE,
			ask            DOUBLE,
			high           DOUBLE,
			low            DOUBLE,
			open           DOUBLE,
			close          DOUBLE,
			volume         DOUBLE,
			change         DOUBLE,
			change_percent DOUBLE
		) TIMESTAMP(timestamp) PARTITION BY DAY
		DEDUP UPSERT KEYS(timestamp, exchange, symbol);
	`

	if _, err := q.db.ExecContext(ctx, createTickersTable); err != nil {
		return ErrQueryError("failed to create tickers table", err)
	}

	// 创建MiniTicker数据表
	createMiniTickersTable := `
		CREATE TABLE IF NOT EXISTS mini_tickers (
			exchange     SYMBOL,
			symbol       SYMBOL,
			timestamp    TIMESTAMP,
			open         DOUBLE,
			high         DOUBLE,
			low          DOUBLE,
			close        DOUBLE,
			volume       DOUBLE,
			quote_volume DOUBLE
		) TIMESTAMP(timestamp) PARTITION BY DAY
		DEDUP UPSERT KEYS(timestamp, exchange, symbol);
	`

	if _, err := q.db.ExecContext(ctx, createMiniTickersTable); err != nil {
		return ErrQueryError("failed to create mini_tickers table", err)
	}

	logger.Ctx(ctx).Info("数据库表结构初始化完成")
	return nil
}

// 统计记录方法

// recordConnectionAttempt 记录连接尝试
func (q *QuestDBStorage) recordConnectionAttempt() {
	q.stats.mu.Lock()
	defer q.stats.mu.Unlock()
	q.stats.ConnectionCount++
	q.stats.LastConnectionTime = time.Now()
}

// recordReconnection 记录重连
func (q *QuestDBStorage) recordReconnection() {
	q.stats.mu.Lock()
	defer q.stats.mu.Unlock()
	q.stats.ReconnectionCount++
	q.stats.LastConnectionTime = time.Now()
}

// recordSuccessfulOperation 记录成功操作
func (q *QuestDBStorage) recordSuccessfulOperation() {
	q.stats.mu.Lock()
	defer q.stats.mu.Unlock()
	q.stats.TotalOperations++
	q.stats.SuccessfulOps++
}

// recordFailedOperation 记录失败操作
func (q *QuestDBStorage) recordFailedOperation(message string) {
	q.stats.mu.Lock()
	defer q.stats.mu.Unlock()
	q.stats.TotalOperations++
	q.stats.FailedOps++
	q.stats.LastError = time.Now()
	q.stats.LastErrorMessage = message
}

// updateAverageLatency 更新平均延迟
func (q *QuestDBStorage) updateAverageLatency(latency time.Duration) float64 {
	newLatencyMs := float64(latency.Milliseconds())

	// 使用简单的移动平均
	if q.stats.AverageLatencyMs == 0 {
		q.stats.AverageLatencyMs = newLatencyMs
	} else {
		q.stats.AverageLatencyMs = (q.stats.AverageLatencyMs*0.9 + newLatencyMs*0.1)
	}

	return q.stats.AverageLatencyMs
}

// GetStorageStats 获取存储统计信息
func (q *QuestDBStorage) GetStorageStats() *StorageStats {
	q.stats.mu.RLock()
	defer q.stats.mu.RUnlock()

	// 返回统计信息的副本
	statsCopy := *q.stats
	return &statsCopy
}

// GetDetailedHealth 获取详细健康信息
func (q *QuestDBStorage) GetDetailedHealth() map[string]interface{} {
	stats := q.GetStorageStats()

	var successRate float64
	if stats.TotalOperations > 0 {
		successRate = float64(stats.SuccessfulOps) / float64(stats.TotalOperations) * 100.0
	}

	return map[string]interface{}{
		"is_running":           q.IsHealthy(),
		"connection_count":     stats.ConnectionCount,
		"reconnection_count":   stats.ReconnectionCount,
		"last_connection_time": stats.LastConnectionTime,
		"total_operations":     stats.TotalOperations,
		"successful_ops":       stats.SuccessfulOps,
		"failed_ops":           stats.FailedOps,
		"success_rate":         successRate,
		"average_latency_ms":   stats.AverageLatencyMs,
		"max_latency_ms":       stats.MaxLatencyMs,
		"klines_saved":         stats.KlinesSaved,
		"trades_saved":         stats.TradesSaved,
		"tickers_saved":        stats.TickersSaved,
		"last_error":           stats.LastError,
		"last_error_message":   stats.LastErrorMessage,
	}
}
