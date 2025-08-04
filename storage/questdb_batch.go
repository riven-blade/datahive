package storage

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"datahive/pkg/logger"
	"datahive/pkg/protocol/pb"

	"go.uber.org/zap"
)

// 估算数据结构的内存大小
const (
	klineSize  = int64(unsafe.Sizeof(pb.Kline{}))
	tradeSize  = int64(unsafe.Sizeof(pb.Trade{}))
	tickerSize = int64(unsafe.Sizeof(pb.Ticker{}))
)

// start 启动批量处理器
func (bp *BatchProcessor) start() error {
	logger.Ctx(context.Background()).Info("启动增强批量处理器",
		zap.Duration("flushInterval", bp.flushInterval),
		zap.Int("maxBatchSize", bp.maxBatchSize),
		zap.Int64("maxMemoryMB", bp.maxMemoryMB))

	bp.wg.Add(1)
	go func() {
		defer bp.wg.Done()
		defer logger.Ctx(context.Background()).Info("批量处理器已停止")

		ticker := time.NewTicker(bp.flushInterval)
		defer ticker.Stop()

		// 内存检查定时器
		memoryTicker := time.NewTicker(5 * time.Second)
		defer memoryTicker.Stop()

		for {
			select {
			case <-ticker.C:
				bp.flush()
			case <-memoryTicker.C:
				bp.checkMemoryUsage()
			case <-bp.stopChan:
				bp.flush() // 最后一次刷新
				return
			}
		}
	}()

	return nil
}

// stop 停止批量处理器
func (bp *BatchProcessor) stop() {
	if bp.stopChan != nil {
		close(bp.stopChan)
		bp.wg.Wait()
	}
}

// addKlines 添加K线数据到批次
func (bp *BatchProcessor) addKlines(klines []*pb.Kline) error {
	bp.klineMu.Lock()
	defer bp.klineMu.Unlock()

	// 检查批次大小
	if len(bp.klineBatch)+len(klines) > bp.maxBatchSize {
		go bp.flushKlines()
	}

	bp.klineBatch = append(bp.klineBatch, klines...)

	// 更新内存使用量
	memoryIncrease := int64(len(klines)) * klineSize
	atomic.AddInt64(&bp.memoryUsage, memoryIncrease)

	// 如果内存使用过多，立即刷新
	if atomic.LoadInt64(&bp.memoryUsage) > bp.maxMemoryMB*1024*1024 {
		go bp.flushKlines()
	}

	return nil
}

// addTrades 添加交易数据到批次
func (bp *BatchProcessor) addTrades(trades []*pb.Trade) error {
	bp.tradeMu.Lock()
	defer bp.tradeMu.Unlock()

	if len(bp.tradeBatch)+len(trades) > bp.maxBatchSize {
		go bp.flushTrades()
	}

	bp.tradeBatch = append(bp.tradeBatch, trades...)

	memoryIncrease := int64(len(trades)) * tradeSize
	atomic.AddInt64(&bp.memoryUsage, memoryIncrease)

	if atomic.LoadInt64(&bp.memoryUsage) > bp.maxMemoryMB*1024*1024 {
		go bp.flushTrades()
	}

	return nil
}

// addTickers 添加价格数据到批次
func (bp *BatchProcessor) addTickers(tickers []*pb.Ticker) error {
	return bp.addTickersWithExchange("", tickers)
}

// addTickersWithExchange 添加价格数据到批次（指定交易所）
func (bp *BatchProcessor) addTickersWithExchange(exchange string, tickers []*pb.Ticker) error {
	bp.tickerMu.Lock()
	defer bp.tickerMu.Unlock()

	if len(bp.tickerBatch)+len(tickers) > bp.maxBatchSize {
		go bp.flushTickersWithExchange(exchange)
	}

	bp.tickerBatch = append(bp.tickerBatch, tickers...)

	memoryIncrease := int64(len(tickers)) * tickerSize
	atomic.AddInt64(&bp.memoryUsage, memoryIncrease)

	if atomic.LoadInt64(&bp.memoryUsage) > bp.maxMemoryMB*1024*1024 {
		go bp.flushTickersWithExchange(exchange)
	}

	return nil
}

// flush 刷新所有批次
func (bp *BatchProcessor) flush() {
	bp.flushKlines()
	bp.flushTrades()
	bp.flushTickers()
}

// checkMemoryUsage 检查内存使用情况
func (bp *BatchProcessor) checkMemoryUsage() {
	currentUsage := atomic.LoadInt64(&bp.memoryUsage)
	maxBytes := bp.maxMemoryMB * 1024 * 1024

	if currentUsage > maxBytes*8/10 { // 80%阈值
		logger.Ctx(context.Background()).Warn("批量处理器内存使用率较高",
			zap.Int64("currentMB", currentUsage/1024/1024),
			zap.Int64("maxMB", bp.maxMemoryMB),
			zap.Float64("usage", float64(currentUsage)/float64(maxBytes)*100))

		// 强制刷新以释放内存
		bp.flush()
	}
}

// flushKlines 刷新K线数据批次
func (bp *BatchProcessor) flushKlines() {
	bp.klineMu.Lock()
	if len(bp.klineBatch) == 0 {
		bp.klineMu.Unlock()
		return
	}

	batch := bp.klineBatch
	bp.klineBatch = make([]*pb.Kline, 0, bp.maxBatchSize)
	bp.klineMu.Unlock()

	// 更新内存使用量
	memoryDecrease := int64(len(batch)) * klineSize
	atomic.AddInt64(&bp.memoryUsage, -memoryDecrease)

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), bp.storage.config.QueryTimeout)
	defer cancel()

	if err := bp.saveKlinesBatch(ctx, batch); err != nil {
		bp.storage.recordFailedOperation("batch klines save failed")
		logger.Ctx(ctx).Error("批量保存K线数据失败",
			zap.Error(err),
			zap.Int("count", len(batch)))
		return
	}

	logger.Ctx(ctx).Debug("批量K线数据已保存",
		zap.Int("count", len(batch)),
		zap.Duration("duration", time.Since(start)))
}

// flushTrades 刷新交易数据批次
func (bp *BatchProcessor) flushTrades() {
	bp.tradeMu.Lock()
	if len(bp.tradeBatch) == 0 {
		bp.tradeMu.Unlock()
		return
	}

	batch := bp.tradeBatch
	bp.tradeBatch = make([]*pb.Trade, 0, bp.maxBatchSize)
	bp.tradeMu.Unlock()

	memoryDecrease := int64(len(batch)) * tradeSize
	atomic.AddInt64(&bp.memoryUsage, -memoryDecrease)

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), bp.storage.config.QueryTimeout)
	defer cancel()

	if err := bp.saveTradesBatch(ctx, batch); err != nil {
		bp.storage.recordFailedOperation("batch trades save failed")
		logger.Ctx(ctx).Error("批量保存交易数据失败",
			zap.Error(err),
			zap.Int("count", len(batch)))
		return
	}

	logger.Ctx(ctx).Debug("批量交易数据已保存",
		zap.Int("count", len(batch)),
		zap.Duration("duration", time.Since(start)))
}

// flushTickers 刷新价格数据批次
func (bp *BatchProcessor) flushTickers() {
	bp.flushTickersWithExchange("")
}

// flushTickersWithExchange 刷新价格数据批次（指定交易所）
func (bp *BatchProcessor) flushTickersWithExchange(exchange string) {
	bp.tickerMu.Lock()
	if len(bp.tickerBatch) == 0 {
		bp.tickerMu.Unlock()
		return
	}

	batch := bp.tickerBatch
	bp.tickerBatch = make([]*pb.Ticker, 0, bp.maxBatchSize)
	bp.tickerMu.Unlock()

	memoryDecrease := int64(len(batch)) * tickerSize
	atomic.AddInt64(&bp.memoryUsage, -memoryDecrease)

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), bp.storage.config.QueryTimeout)
	defer cancel()

	if err := bp.saveTickersBatchWithExchange(ctx, exchange, batch); err != nil {
		bp.storage.recordFailedOperation("batch tickers save failed")
		logger.Ctx(ctx).Error("批量保存价格数据失败",
			zap.Error(err),
			zap.Int("count", len(batch)))
		return
	}

	logger.Ctx(ctx).Debug("批量价格数据已保存",
		zap.Int("count", len(batch)),
		zap.Duration("duration", time.Since(start)))
}

// 批量保存实现

// saveKlinesBatch 批量保存K线数据 - 按时间周期分表
func (bp *BatchProcessor) saveKlinesBatch(ctx context.Context, klines []*pb.Kline) error {
	if len(klines) == 0 {
		return nil
	}

	// 按时间周期和交易所分组
	groups := make(map[string][]*pb.Kline)
	for _, kline := range klines {
		key := fmt.Sprintf("%s_%s", kline.Exchange, kline.Timeframe)
		groups[key] = append(groups[key], kline)
	}

	// 分别插入不同时间周期的表
	for key, group := range groups {
		parts := strings.Split(key, "_")
		if len(parts) < 2 {
			continue
		}
		timeframe := parts[len(parts)-1]

		if err := bp.insertKlinesByTimeframe(ctx, timeframe, group); err != nil {
			return ErrQueryError(fmt.Sprintf("failed to insert klines for timeframe %s", timeframe), err)
		}
	}

	return nil
}

// insertKlinesByTimeframe 插入指定时间周期的K线数据
func (bp *BatchProcessor) insertKlinesByTimeframe(ctx context.Context, timeframe string, klines []*pb.Kline) error {
	if len(klines) == 0 {
		return nil
	}

	tableName := fmt.Sprintf("klines_%s", timeframe)

	var values []string
	var args []interface{}

	for i, kline := range klines {
		values = append(values, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			i*10+1, i*10+2, i*10+3, i*10+4, i*10+5, i*10+6, i*10+7, i*10+8, i*10+9, i*10+10))

		openTime := time.UnixMilli(kline.OpenTime)
		args = append(args,
			kline.Exchange, kline.Symbol, kline.Timeframe, openTime,
			kline.Open, kline.High, kline.Low, kline.Close, kline.Volume, kline.Closed)
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (exchange, symbol, timeframe, open_time, open, high, low, close, volume, closed) VALUES %s",
		tableName, strings.Join(values, ", "))

	_, err := bp.storage.db.ExecContext(ctx, query, args...)
	if err != nil {
		return ErrQueryError(fmt.Sprintf("failed to batch insert %d klines to %s", len(klines), tableName), err)
	}

	return nil
}

// saveTradesBatch 批量保存交易数据
func (bp *BatchProcessor) saveTradesBatch(ctx context.Context, trades []*pb.Trade) error {
	if len(trades) == 0 {
		return nil
	}

	// 分批处理，避免SQL参数过多
	const maxBatchSize = 1000
	for i := 0; i < len(trades); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(trades) {
			end = len(trades)
		}

		batch := trades[i:end]
		if err := bp.insertTradesBatch(ctx, batch); err != nil {
			return ErrQueryError(fmt.Sprintf("failed to insert trades batch [%d:%d]", i, end), err)
		}
	}

	return nil
}

// insertTradesBatch 插入交易数据批次
func (bp *BatchProcessor) insertTradesBatch(ctx context.Context, trades []*pb.Trade) error {
	if len(trades) == 0 {
		return nil
	}

	var values []string
	var args []interface{}

	for i, trade := range trades {
		values = append(values, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			i*7+1, i*7+2, i*7+3, i*7+4, i*7+5, i*7+6, i*7+7))

		ts := time.UnixMilli(trade.Timestamp)
		args = append(args,
			trade.Exchange, trade.Symbol, trade.Id, trade.Price, trade.Quantity, trade.Side, ts)
	}

	query := fmt.Sprintf(
		"INSERT INTO trades (exchange, symbol, id, price, quantity, side, timestamp) VALUES %s",
		strings.Join(values, ", "))

	_, err := bp.storage.db.ExecContext(ctx, query, args...)
	if err != nil {
		return ErrQueryError(fmt.Sprintf("failed to batch insert %d trades", len(trades)), err)
	}

	return nil
}

// saveTickersBatch 批量保存价格数据
func (bp *BatchProcessor) saveTickersBatch(ctx context.Context, tickers []*pb.Ticker) error {
	return bp.saveTickersBatchWithExchange(ctx, "", tickers)
}

// saveTickersBatchWithExchange 批量保存价格数据（指定交易所）
func (bp *BatchProcessor) saveTickersBatchWithExchange(ctx context.Context, exchange string, tickers []*pb.Ticker) error {
	if len(tickers) == 0 {
		return nil
	}

	var values []string
	var args []interface{}

	for i, ticker := range tickers {
		values = append(values, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			i*13+1, i*13+2, i*13+3, i*13+4, i*13+5, i*13+6, i*13+7, i*13+8, i*13+9, i*13+10, i*13+11, i*13+12, i*13+13))

		ts := time.UnixMilli(ticker.Timestamp)

		args = append(args,
			exchange, ticker.Symbol, ts,
			ticker.Last, ticker.Bid, ticker.Ask, ticker.High, ticker.Low,
			ticker.Open, ticker.Close, ticker.Volume, ticker.Change, ticker.ChangePercent)
	}

	query := fmt.Sprintf(
		"INSERT INTO tickers (exchange, symbol, timestamp, last, bid, ask, high, low, open, close, volume, change, change_percent) VALUES %s",
		strings.Join(values, ", "))

	_, err := bp.storage.db.ExecContext(ctx, query, args...)
	if err != nil {
		return ErrQueryError(fmt.Sprintf("failed to batch insert %d tickers", len(tickers)), err)
	}

	return nil
}
